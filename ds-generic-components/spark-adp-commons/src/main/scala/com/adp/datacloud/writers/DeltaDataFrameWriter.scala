package com.adp.datacloud.writers

import com.adp.datacloud.ds.hive.hiveTableUtils
import io.delta.tables._
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.DecimalType

case class DataFrameWriterPartitionConfig(partitionSpec: Option[String]) {

  // TODO: convert columns to lower case
  lazy val partitionColumns = if (partitionSpec.isDefined && partitionSpec.get.nonEmpty) {
    partitionSpec.get.split(",").toList
  } else List.empty[String]

  lazy val staticPartitionColumns =
    partitionColumns
      .filter(_.contains("="))

  lazy val partitionColumnNames = partitionColumns
    .map(token => {
      if (token.contains("=")) {
        // get column names from static partitions
        token.split("=")(0)
      } else {
        token
      }
    })

}

object DeltaDataFrameWriter {

  private val logger = Logger.getLogger(getClass())

  val DeltaDataFrameWriterVersion: String = getClass.getPackage.getImplementationVersion

  def patchDataFrameWithStaticPartitions(
      inputDf: DataFrame,
      staticPartitionMap: List[String]) = {
    // add static partitions to the dataframe as constant columns
    val df = staticPartitionMap.foldLeft(inputDf)((dataFrame, y) => {
      val columnName      = y.split("=")(0)
      val columnValue     = y.split("=")(1)
      val exp: Expression = CatalystSqlParser.parseExpression(columnValue)
      if (exp.resolved) {
        val columnDataType    = exp.dataType
        val columnParsedValue = exp.toString()
        dataFrame.withColumn(columnName, lit(columnParsedValue).cast(columnDataType))
      } else {
        dataFrame.withColumn(columnName, lit(columnValue))
      }
    })
    df
  }


  def createTableIfNotExists(
      df: DataFrame,
      tableName: String,
      partitionColumnNames: List[String])(implicit sparkSession: SparkSession): Unit = {
    val exists_or_not = sparkSession.catalog.tableExists(tableName)

    if (!sparkSession.catalog.tableExists(tableName)) {
      val emptyTable = df.where("1=0")

      logger.info(s"CREATE_DELTA_TABLE: creating delta table by the name $tableName")
      if (partitionColumnNames.nonEmpty) {
        emptyTable.write
          .format("delta")
          .partitionBy(partitionColumnNames: _*)
          .saveAsTable(tableName)
      } else {
        emptyTable.write.format("delta").saveAsTable(tableName)
      }
      //TODO: figure out set property to delta table
      val setPropertySql =
        s"ALTER TABLE $tableName SET TBLPROPERTIES ('DeltaDataFrameWriter.tablecreate.ver'='$DeltaDataFrameWriterVersion')"
      logger.info(setPropertySql)
      sparkSession.sql(setPropertySql)
      sparkSession.sql(s"DESCRIBE EXTENDED $tableName").show(100, false)
    } else {
      val tablePath = hiveTableUtils.getTableLocation(tableName)
      if (!DeltaTable.isDeltaTable(sparkSession, tablePath)) {
        // https://docs.databricks.com/delta/delta-utility.html#convert-to-delta&language-scala
        val partitionNames = hiveTableUtils.getTablePartitionNames(tableName)
        if (partitionNames.isEmpty)
          DeltaTable.convertToDelta(sparkSession, s"parquet.`$tablePath`")
      }
    }
    

  }

  def convertPartitionSpecToLowerCase(partitionSpec: Option[String]): Option[String] = {
    if (partitionSpec.isDefined) {
      val partitionSpecString = partitionSpec.get
      val lowercaseString = partitionSpecString
        .split(",")
        .map(x => {
          if (x.contains('=')) {
            val tokens = x.split("=")
            List(tokens(0).toLowerCase, tokens(1)).mkString("=")
          } else {
            x.toLowerCase
          }
        })
        .mkString(",")
      Some(lowercaseString)
    } else {
      partitionSpec
    }
  }

  def conformSchema(
      df: DataFrame,
      tableName: String)(implicit sparkSession: SparkSession): DataFrame = {
    if (sparkSession.catalog.tableExists(tableName)) {
      val targetSchema = sparkSession.table(tableName).schema
      val targetColumns = targetSchema.fields.map(f => f.name -> f.dataType).toMap

      df.columns.foldLeft(df) { (currentDf, colName) =>
        val sourceDataType = currentDf.schema(colName).dataType
        targetColumns.get(colName) match {
          // Only attempt to cast if the source type is Decimal and the target type is different.
          case Some(targetType) if sourceDataType.isInstanceOf[DecimalType] && sourceDataType != targetType =>
            try {
              logger.info(
                s"Casting source decimal column '$colName' from $sourceDataType to target type $targetType for table '$tableName'")
              currentDf.withColumn(colName, col(colName).cast(targetType))
            } catch {
              case e: Throwable =>
                logger.warn(
                  s"Could not cast column '$colName' to $targetType. Proceeding with original type. Error: ${e.getMessage}")
                currentDf
            }
          case _ =>
            currentDf // No change if column not in target, types already match, or source is not Decimal.
        }
      }
    } else {
      df // Return original df if table doesn't exist yet
    }
  }

  def getLowerCasedDf(df: DataFrame): DataFrame = {
    df.toDF(df.columns.map(_.toLowerCase()): _*)
  }

  def insert(
      tableName: String,
      inputDf: DataFrame,
      isOverWrite: Boolean,
      partitionSpec: Option[String] = None)(implicit sparkSession: SparkSession) = {

    val lowerCasedPartitionSpec = convertPartitionSpecToLowerCase(partitionSpec)
    val partitionConfig         = DataFrameWriterPartitionConfig(lowerCasedPartitionSpec)
    val lowerCasedInputDf       = getLowerCasedDf(inputDf)
    val df =
      patchDataFrameWithStaticPartitions(
        lowerCasedInputDf,
        partitionConfig.staticPartitionColumns)

    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sparkSession.conf
      .set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact ", true)
    sparkSession.conf
      .set("spark.databricks.delta.schema.autoMerge.enabled", true)

    val sc = sparkSession.sparkContext

    val OverWriteMode = if (isOverWrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }

    logger.info(
      s"INSERT_INTO_TABLE_CONFIG: writing to table " + tableName
        + " with config \nPARTITION_SPECIFICATION=" + partitionConfig.partitionColumns
        + "\nPARTITION_COLUMNS=" + partitionConfig.partitionColumnNames +
        "\nOVERWRITE_MODE=" + OverWriteMode)

    // check if create table is even required
    createTableIfNotExists(df, tableName, partitionConfig.partitionColumnNames)

    // Conform the DataFrame schema to the target table's schema before writing
    val conformDf = conformSchema(df, tableName)
    val t0 = System.currentTimeMillis()
    if (partitionConfig.partitionColumnNames.nonEmpty) {

      if (isOverWrite) {
        deleteOverWritingPartitions(tableName, lowerCasedPartitionSpec.get, conformDf)
      }
      conformDf.write
        .format("delta")
        .option("mergeSchema", true)
        .mode(SaveMode.Append)
        .partitionBy(partitionConfig.partitionColumnNames: _*)
        .saveAsTable(tableName)
    } else {
      conformDf.write
        .format("delta")
        .option("mergeSchema", true)
        .mode(OverWriteMode)
        .saveAsTable(tableName)
    }

    val writeTimeSecs = (System.currentTimeMillis() - t0) / 1000
    logger.info(s"$tableName was witten in $writeTimeSecs seconds")

    DataCloudDataFrameWriterLog(
      sc.applicationId,
      sc.applicationAttemptId,
      tableName,
      0,
      0,
      0,
      writeTimeSecs.toInt)
  }

  /**
   * convert dynamic insert overwrite into delete and append. this is a work around until delta format has a fix.
   * [[https://community.databricks.com/s/question/0D53f00001HKHmsCAH/dynamic-partition-overwrite-for-delta-tables]]
   * @param tableName
   * @param sparkSession
   * @param lowerCasedPartitionSpec
   * @param df
   */
  def deleteOverWritingPartitions(
      tableName: String,
      partitionString: String,
      df: DataFrame)(implicit sparkSession: SparkSession): Unit = {

    import sparkSession.implicits._
    val distinctColExpr = concat_ws(
      " AND ",
      partitionString
        .split(",")
        .map(x => {
          if (x.contains("=")) {
            val tokens = x.split("=")
            if (tokens(1).startsWith("'")) lit(x)
            else {
              lit(List(tokens(0), "='", tokens(1), "'").mkString)
            }
          } else concat(lit(s"$x"), lit(" = '"), col(x), lit("'"))
        }): _*)

    val whereString = df
      .select(distinctColExpr)
      .distinct()
      .map(_.getString(0))
      .collect
      .mkString("(", ") OR (", ")")

    logger.info(s"OVERWRITE_REPLACE_WHERE_STRING: $whereString")

    val deltaTableLocation = hiveTableUtils.getTableLocation(tableName)
    val deltaTable         = DeltaTable.forPath(sparkSession, deltaTableLocation)
    deltaTable.delete(whereString)
  }

  def insertOverwrite(
      tableName: String,
      df: DataFrame,
      partitionSpec: Option[String] = None)(implicit
      sparkSession: SparkSession): DataCloudDataFrameWriterLog = {
    insert(tableName, df, true, partitionSpec)
  }

   

}
