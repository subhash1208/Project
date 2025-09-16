package com.adp.datacloud.dynamo

import com.adp.datacloud.cli.{DynamoIngestorConfig, dynamoIngestorOptions}
import com.adp.datacloud.writers.HiveDataFrameWriter
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model._
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

import java.util.HashMap
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object dynamoDataIngestor {

  private val logger = Logger.getLogger(getClass())

  val region = if (Regions.getCurrentRegion() != null) {
    Regions.getCurrentRegion.getName
  } else {
    "us-east-1"
  }

  def getDynamoType(sparkType: Any): ScalarAttributeType = {
    sparkType match {
      case StringType  => ScalarAttributeType.S
      case IntegerType => ScalarAttributeType.N
      case FloatType   => ScalarAttributeType.N
      case DoubleType  => ScalarAttributeType.N
      case BooleanType => ScalarAttributeType.B
      case _           => ScalarAttributeType.S
    }
  }

  def main(args: Array[String]) {
    val config = dynamoIngestorOptions.parse(args)

    val tableName = config.file.split("/").last

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(s"DYNAMO_${tableName}")
      .enableHiveSupport()
      .getOrCreate()

    val filesString = config.files
      .getOrElse("")
      .trim

    if (filesString.nonEmpty) {
      filesString
        .split(",")
        .foreach(x => {
          logger.info(s"DEPENDENCY_FILE: adding $x to sparkFiles")
          sparkSession.sparkContext.addFile(x)
        })
    }

    ingest(config)
  }

  def createDynamoDBTable(
      ingestorConfig: DynamoIngestorConfig,
      schema: StructType): Unit = {

    val tableName    = ingestorConfig.dynamodbTableName
    val partitionKey = ingestorConfig.partitionKey
    val sortKey      = ingestorConfig.sortKey

    val client = AmazonDynamoDBClientBuilder.defaultClient()

    val dynamoDB = new DynamoDB(client);
    try {
      val dynamoTable = client.describeTable(tableName).getTable
      logger.info("Table already exists. Not creating new table " + dynamoTable)
    } catch {
      case rnf: ResourceNotFoundException => {

        val partitionKeyDataType =
          schema.fields.filter(x => x.name == partitionKey).head.dataType
        val partitionKeyAttributeDefinition =
          new AttributeDefinition(partitionKey, getDynamoType(partitionKeyDataType))
        val attributeDefinitions = sortKey match {
          case Some(x) => {
            val sortKeyDataType = schema.fields.filter(y => y.name == x).head.dataType
            List(
              partitionKeyAttributeDefinition,
              new AttributeDefinition(x, getDynamoType(sortKeyDataType)))
          }
          case None => List(partitionKeyAttributeDefinition)
        }

        val partitionKeySchema = new KeySchemaElement(partitionKey, KeyType.HASH)
        val keySchema = sortKey match {
          case Some(x) => List(partitionKeySchema, new KeySchemaElement(x, KeyType.RANGE))
          case None    => List(partitionKeySchema)
        }

        val createTableRequestBase = (new CreateTableRequest())
          .withTableName(tableName)
          .withAttributeDefinitions(attributeDefinitions.toSeq)
          .withKeySchema(keySchema)

        val createTableRequest = if (ingestorConfig.isProvisioned) {
          createTableRequestBase
            .withBillingMode(BillingMode.PROVISIONED)
            .withProvisionedThroughput(
              new ProvisionedThroughput(
                ingestorConfig.readThroughput,
                ingestorConfig.writeThroughput))
        } else {
          createTableRequestBase.withBillingMode(BillingMode.PAY_PER_REQUEST)
        }

        val result = client.createTable(createTableRequest)
        logger.info("Created table " + result.getTableDescription().getTableName())

      }
    }

  }

  /**
   * Ingests data into a hive table in RAW database first and then into ElasticSearch
   */
  def ingest(config: DynamoIngestorConfig)(implicit sparkSession: SparkSession): Unit = {

    logger.info("DYNAMO_DB_INPUT_SQL = " + config.sql)
    val df = sparkSession.sql(config.sql).repartition(config.writeParallelism).persist()

    // validate if the dataframe is correct without empty values in keys
    val filtered = df
      .filter(col(config.partitionKey).isNotNull)
      .filter({
        config.sortKey match {
          case Some(x) => col(x).isNotNull
          case None    => lit(true)
        }
      })

    val keys = config.sortKey match {
      case Some(x) => List(config.partitionKey, x)
      case None    => List(config.partitionKey)
    }

    assert(
      df.select(keys map { col(_) }: _*).distinct.count() == df.count,
      "Duplicates found in data. Check your partition and sort keys")
    assert(filtered.count() == df.count(), "Null values found in key columns. Aborting")

    val rawDbName = config.getParam("__GREEN_RAW_DB__")
    rawDbName match {
      case Some(x) => {
        val hiveWriter = HiveDataFrameWriter("parquet", None)
        hiveWriter.insertSafely(
          s"${x}.dyn_${config.dynamodbTableName.replace("-", "_")}",
          df,
          true)
      }
      case None => logger.warn("RAW DB not found. Not Saving the data to hive.")
    }

    // Check if the table exists in dynamodb. If not create it.
    createDynamoDBTable(config, df.schema)

    var jobConf = new JobConf(sparkSession.sparkContext.hadoopConfiguration)
    jobConf.set("dynamodb.output.tableName", config.dynamodbTableName)
    jobConf.set(
      "mapred.output.format.class",
      "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set(
      "mapred.input.format.class",
      "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    jobConf.set(
      "dynamodb.customAWSCredentialsProvider",
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    jobConf.set("dynamodb.servicename", "dynamodb")
    jobConf.set("dynamodb.endpoint", s"dynamodb.${region}.amazonaws.com")
    jobConf.set("dynamodb.regionid", region)

    config.dynamoConfiguration.foreach({
      case (key, value) => {
        jobConf.set(key, value)
      }
    })

    val fieldsWithIndex = df.schema.fields.zipWithIndex
    val df_rdd = config.sortKey match {
      case Some(x) =>
        df.repartition(col(config.partitionKey))
          .sortWithinPartitions(x)
          .rdd // This can improve write performance
      case None => df.repartition(col(config.partitionKey)).rdd
    }
    val formattedRDD = df_rdd.map(a => {
      var ddbMap = new HashMap[String, AttributeValue]()
      fieldsWithIndex.foreach(field => {
        val dataType   = field._1.dataType
        val fieldName  = field._1.name
        val fieldIndex = field._2

        def processField(data: Any, dataType: DataType): Option[AttributeValue] = {
          if (data != null) {
            dataType match {
              case StringType => Some(new AttributeValue(data.asInstanceOf[String]))
              case IntegerType | DoubleType | FloatType | LongType =>
                Some(new AttributeValue().withN(data.toString))
              case DateType | TimestampType =>
                Some(new AttributeValue().withS(data.toString()))
              case BooleanType =>
                Some(new AttributeValue().withBOOL(data.asInstanceOf[Boolean]))
              case ArrayType(someType, _) => {
                val listOfValues = data.asInstanceOf[Seq[Any]]
                if (!listOfValues.isEmpty) {
                  val converted = listOfValues
                    .map(x => processField(x, someType))
                    .filter(_.isDefined)
                    .map(_.get)
                    .asJavaCollection
                  if (!converted.isEmpty) {
                    Some((new AttributeValue()).withL(converted))
                  } else None
                } else None
              }
              case StructType(someSeqOfStructFields) => {
                val structFieldTypesMap =
                  someSeqOfStructFields.map(x => x.name -> x.dataType).toMap
                val row = data.asInstanceOf[Row]
                val mapOfValues = someSeqOfStructFields.zipWithIndex
                  .map(x => x._1.name -> row.get(x._2))
                  .toMap
                if (!mapOfValues.isEmpty) {
                  val converted = mapOfValues
                    .map(
                      x =>
                        x._1.toString -> processField(
                          x._2,
                          structFieldTypesMap(x._1.toString())))
                    .filter(_._2.isDefined)
                    .map(x => x._1 -> x._2.get)
                  if (!converted.isEmpty) {
                    Some((new AttributeValue()).withM(converted))
                  } else None
                } else None
              }
              case MapType(someKeyType, someValueType, _) => {
                val mapOfValues = data.asInstanceOf[Map[Any, Any]]
                if (!mapOfValues.isEmpty) {
                  val converted = mapOfValues
                    .map(x => x._1.toString -> processField(x._2, someValueType))
                    .filter(_._2.isDefined)
                    .map(x => x._1 -> x._2.get)
                  if (!converted.isEmpty) {
                    Some((new AttributeValue()).withM(converted))
                  } else None
                } else None
              }
            }
          } else None
        }
        val value = processField(a.get(fieldIndex), dataType)
        value match {
          case Some(x) => ddbMap.put(fieldName, x)
          case None    => Unit
        }
      })

      var item = new DynamoDBItemWritable()
      item.setItem(ddbMap)

      (new Text(""), item)
    })

    formattedRDD.saveAsHadoopDataset(jobConf)

  }

}
