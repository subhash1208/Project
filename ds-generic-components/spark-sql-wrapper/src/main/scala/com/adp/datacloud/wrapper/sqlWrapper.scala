package com.adp.datacloud.wrapper

import com.adp.datacloud.cli.{SqlWrapperConfig, sqlWrapperOptions}
import com.adp.datacloud.ds.udfs.{UDFUtility, Word2VecEmbeddings}
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, RepartitionByExpression, Sort}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.util.matching.Regex
import com.adp.datacloud.ds.udfs.JobFunctionsUDF

case class HiveDataframeWriterConfig(
  tableName:   String,
  partitionBy: Option[String],
  clusterBy:   Option[String],
  insertSql:   String,
  isOverWrite: Boolean        = true,
  createTable: Boolean        = false,
  format:      String         = s"parquet")

object sqlWrapper {
  private val logger = Logger.getLogger(getClass())

  // (?s) is for multiline match and (?i) is for case insensitive match
  val selectExtractPattern: Regex = "(?s)(?i)^.*?(select.*)".r

  def hash(value: String): String = {
    import org.apache.commons.codec.binary.Hex
    import org.apache.commons.codec.digest.DigestUtils
    DigestUtils.sha256Hex(
      Hex.decodeHex(
        ("40" + new String(Hex.encodeHex(value.getBytes("cp500"))).toUpperCase + "4B").toCharArray())).toUpperCase.take(8)
  }

  def downloadFile(url: String, filepath: String): Unit = {
    import java.io.File
    import java.net.URL

    import org.apache.commons.io.IOUtils
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{ FileSystem, Path }

    import scala.sys.process._
    // First download file to local path
    new URL(url) #> new File(filepath) !!
    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    // Copy the file over to HDFS
    val inputStream = new java.io.FileInputStream(filepath)
    val outputStream = hdfs.create(new Path(filepath))
    val numBytes = IOUtils.copy(inputStream, outputStream)
    inputStream.close()
    outputStream.close()
    logger.info(s"Saved to HDFS ${numBytes} bytes for file " + url)
  }

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val sqlConfig = sqlWrapperOptions.parse(args)

    implicit val sparkSession: SparkSession = SparkSession.builder().appName(sqlConfig.file match {
      case Some(x) => x
      case None    => "Custom sql with sql-wrapper"
    }).enableHiveSupport().getOrCreate()

    val filesString = sqlConfig.files
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

    executeInputSql(sqlConfig)
  }

  def executeInputSql(sqlConfig: SqlWrapperConfig)(implicit sparkSession: SparkSession): Unit = {

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    println(s"SQL_WRAPPER_CONFIG: ${write(sqlConfig)}")

    UDFUtility.registerUdfs
    Word2VecEmbeddings.registerUdfs
    JobFunctionsUDF.registerUdfs

    // Naive separation using semi-colons to identify individual SQL commands
    sqlConfig.sql.split(";").foreach {
      query =>
        {
          val pattern = """(json|csv|parquet).`(http[^\s]+)`""".r
          val urlMap = pattern.findAllIn(query).matchData.map({
            m =>
              {
                val url = m.group(2)
                val key = hash(url)
                (s"/tmp/${key}.metadata", url)
              }
          }).toMap

          urlMap.foreach({
            case (filePath: String, url: String) => {
              downloadFile(url, filePath)
            }
          })

          val modifiedQuery = urlMap.foldLeft(query)((sql, urlTuple) => {
            sql.replaceAll(urlTuple._2, urlTuple._1)
          })

          processSql(sqlConfig, modifiedQuery)
        }
    }
  }

  def processSql(sqlConfig: SqlWrapperConfig, sqlQuery: String)(
    implicit
    sparkSession: SparkSession): Any = {
    logger.info(s"INPUT_SQL: $sqlQuery")
    println(s"INPUT_SQL: $sqlQuery")
    val config = if (sqlConfig.optimizeInsert) {
      getHiveDataframeWriterConfigFromQuery(sqlQuery, selectExtractPattern)
    } else {
      None
    }
    if (config.isDefined) {
      val writerConfig = config.get
      logger.info(s"HIVE_DATA_FRAME_WRITER_CONFIG: $writerConfig")
      val hiveWriter = HiveDataFrameWriter(
        writerConfig.format,
        writerConfig.partitionBy,
        writerConfig.clusterBy)
      val dataFrame = sparkSession.sql(writerConfig.insertSql)
      val targetSchema = if (writerConfig.createTable) {
        hiveWriter.createTableIfNotExists(
          writerConfig.tableName,
          dataFrame.schema,
          None,
          skipColumnSort = true)
      } else {
        sparkSession.table(writerConfig.tableName).schema
      }

      // convert to lower case and remove static partition column names from table schema
      val targetColumnNames = targetSchema.fieldNames
        .map(_.toLowerCase())
        .filterNot(hiveWriter.getStaticPartitionCols.contains)

      val columnNamesMap =
        dataFrame.schema.fieldNames.zip(targetColumnNames).toMap

      logger.info(s"DATA_FRAME_RENAME_MAP:$columnNamesMap")
      // rename input dataframe columns according to target table.
      // this is  required especially when you have case statements in the input sql
      val renamedDf = dataFrame.toDF(targetColumnNames: _*)

      val writeLog = hiveWriter.optimizeForS3WriteAndInsertData(
        writerConfig.tableName,
        renamedDf,
        renamedDf.schema,
        writerConfig.isOverWrite)
      println(s"OPTIMISED_INSERT_LOG: $writeLog")
      logger.info(s"OPTIMISED_INSERT_LOG: $writeLog")
    } else {
      sparkSession.sql(sqlQuery).show
    }
  }

  /**
   * converts a sql to HiveDataframeWriter config.
   * all CTAS in all formats and insert into for parquet tables are supported
   * @param sqlQuery
   * @param selectQueryRegex
   * @param sparkSession
   * @return
   */
  def getHiveDataframeWriterConfigFromQuery(
    sqlQuery:         String,
    selectQueryRegex: Regex)(implicit sparkSession: SparkSession): Option[HiveDataframeWriterConfig] = {
    val parsedPlan: LogicalPlan =
      sparkSession.sessionState.sqlParser.parsePlan(sqlQuery)

    /**
     * extracts distribute by/cluster by/sort by columns from query
     * @param sqlPlan
     * @return
     */
    def extractClusterByFromPlan(sqlPlan: LogicalPlan) = {
      // TODO: handle scenarios where distribute clause contains constants like 'distribute by 1'
      sqlPlan.nodeName match {
        case "Sort" => {
          // TODO: handle sort order
          val sortPlan = sqlPlan.asInstanceOf[Sort]
          val repartitionByExpressionPlan =
            sortPlan.child.asInstanceOf[RepartitionByExpression]
          // TODO: find a simpler solution to extract column names
          val distributeByCols =
            repartitionByExpressionPlan.partitionExpressions
              .map(_.toString().replaceAll("'", "").toLowerCase())
              .mkString(",")
          Some(distributeByCols)
        }
        case "RepartitionByExpression" => {
          val repartitionByExpressionPlan =
            sqlPlan.asInstanceOf[RepartitionByExpression]
          // TODO: find a simpler solution to extract column names
          val distributeByCols =
            repartitionByExpressionPlan.partitionExpressions
              .map(_.toString().replaceAll("'", "").toLowerCase())
              .mkString(",")
          Some(distributeByCols)
        }
        case _ => {
          None
        }
      }
    }

    parsedPlan.nodeName match {
      case "InsertIntoStatement" => {
        // TODO: support non parquet tables
        val insertIntoPlan = parsedPlan.asInstanceOf[InsertIntoStatement]
        val targetTablePlan =
          insertIntoPlan.table.asInstanceOf[UnresolvedRelation]
        val targetTableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(targetTablePlan.tableName)
        val targetTableName = targetTablePlan.tableName
        val targetCatalogTable = sparkSession.sessionState.catalog
          .getTableMetadata(targetTableIdentifier)
        val selectPlan = EliminateSubqueryAliases.apply(insertIntoPlan.query)
        val partitionBy = if (insertIntoPlan.partitionSpec.nonEmpty) {
          val partitionSpec = insertIntoPlan.partitionSpec
            .map {
              case (partitionName, partitionValue) => {
                if (partitionValue.isDefined) {
                  s"$partitionName=${partitionValue.get}"
                } else {
                  partitionName
                }
              }
            }
            .mkString(",")
          Some(partitionSpec)
        } else {
          None
        }
        if (targetCatalogTable.provider
          .toString()
          .toUpperCase()
          .contains("PARQUET") || targetCatalogTable.storage.serde
          .toString()
          .toUpperCase()
          .contains("PARQUET")) {
          val clusterBy = extractClusterByFromPlan(selectPlan)
          val selectQueryRegex(selectSql) = sqlQuery

          Some(
            HiveDataframeWriterConfig(
              targetTableName,
              partitionBy,
              clusterBy,
              selectSql,
              insertIntoPlan.overwrite))
        } else {
          None
        }
      }
      case "CreateTable" => {
        //TODO: handle table if not exists checks from sql
        //TODO: propagate table properties from CTAS sql
        //TODO: support non parquet tables
        val createTablePlan = parsedPlan.asInstanceOf[CreateTable]
        val targetCatalogTable = createTablePlan.tableDesc
        if ((targetCatalogTable.provider
          .toString()
          .toUpperCase()
          .contains("PARQUET") || targetCatalogTable.storage.serde
          .toString()
          .toUpperCase()
          .contains("PARQUET")) && createTablePlan.query.isDefined) {
          val selectPlan =
            EliminateSubqueryAliases.apply(createTablePlan.query.get)
          val targetTableName =
            targetCatalogTable.identifier.toString().replaceAll("`", "")
          val partitionBy =
            if (createTablePlan.tableDesc.partitionColumnNames.nonEmpty) {
              Some(createTablePlan.tableDesc.partitionColumnNames.mkString(","))
            } else {
              None
            }
          val clusterBy = extractClusterByFromPlan(selectPlan)
          val selectQueryRegex(selectSql) = sqlQuery
          Some(
            HiveDataframeWriterConfig(
              targetTableName,
              partitionBy,
              clusterBy,
              selectSql,
              isOverWrite = false,
              createTable = true))
        } else {
          None
        }
      }
      case "DropTableCommand" => {
        //TODO: parllelize file drop
        None
      }
      case _ => {
        None
      }
    }

  }

}