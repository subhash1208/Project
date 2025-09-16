package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{
  DxrParallelDataExtractorConfig,
  dxrStagingDataProcessorConfig,
  dxrStagingDataProcessorOptions
}
import com.adp.datacloud.ds.DXRDataExtractor
import com.adp.datacloud.ds.util._
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit, when, col}
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

import java.io.{PrintWriter, StringWriter}
import scala.collection.JavaConversions._

/**
 * This program reloads the data retained in the HDFS staging area from a previous dxr execution.. into the Hive Tables in Landing, Blue and Green
 */

object dxrStagingDataProcessor extends DXRDataExtractor {

  println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {
    val rerunConfig = dxrStagingDataProcessorOptions.parse(args)

    implicit val sparkSession: SparkSession =
      SparkSession
        .builder()
        .appName(rerunConfig.applicationName)
        .enableHiveSupport()
        .getOrCreate()

    processFailedJob(rerunConfig)
  }

  def processFailedJob(config: dxrStagingDataProcessorConfig)(implicit
      sparkSession: SparkSession): Unit = {

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    logger.info(s"DXR_CONNECTION_STRING = ${config.dxrJdbcUrl}")
    logger.info(s"DXR_JOB_CONFIG_SQL_STRING = ${config.jobConfigSql}")

    logger.debug(write(sparkSession.conf.getAll))

    val dxrLogger =
      DXRLogger(
        config,
        List[String](),
        config.inputParams.getOrElse("env", ""),
        config.applicationId)

    dxrLogger.insertStartStatusLog(None)

    try {

      val (dxrJobLog: DXRJobLog, dxrConfig: DxrParallelDataExtractorConfig) =
        getFailedJobConfig(config)

      if (config.recoverMode) {
        val failedWrites: String =
          recoverFailedWrites(config, dxrLogger, dxrJobLog, dxrConfig)
        // log job as failure if an sql has zero records in total or if there were errors during extracts/writes
        if (failedWrites.nonEmpty) {
          throw DXR2Exception(s"FAILED_WRITE_SQLS=$failedWrites")
        } else {
          dxrLogger.updateStatusLog(None, "FINISHED", 2, "")
        }
      } else {
        rerunFailedConnections(dxrLogger, config, dxrJobLog, dxrConfig)
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        dxrLogger.updateStatusLog(None, "FAILED", 4, sw.toString, isError = true)
        // rethrow the error to mark the job as failure
        throw e
    }
  }

  def rerunFailedConnections(
      dxrLogger: DXRLogger,
      config: dxrStagingDataProcessorConfig,
      dxrJobLog: DXRJobLog,
      dxrConfig: DxrParallelDataExtractorConfig)(implicit
      sparkSession: SparkSession): Dataset[DXRTaskLog] = {

    val connectionsDS =
      getFailedConnectionsDS(config, dxrJobLog, dxrConfig)

    dxrParallelDataExtractor.extractDataByConnections(
      dxrConfig,
      dxrLogger,
      connectionsDS,
      connectionsDS)
  }

  def getFailedConnectionsDS(
      config: dxrStagingDataProcessorConfig,
      dxrJobLog: DXRJobLog,
      dxrConfig: DxrParallelDataExtractorConfig)(implicit
      sparkSession: SparkSession): Dataset[DXRConnection] = {

    import sparkSession.implicits._

    sparkSession.read
      .jdbc(config.dxrJdbcUrl, config.jobLogsSql, config.dxrConnectionProperties)
      .printSchema()

    val sqlNames = sparkSession.read
      .jdbc(config.dxrJdbcUrl, config.jobLogsSql, config.dxrConnectionProperties)
      .select("SQL_NAME")
      .distinct()
      .collect()
      .map(_.getAs[String]("SQL_NAME"))
      .toList

    val sqlStringsMap: Map[String, String] = sqlNames.map(x => x -> "").toMap

    val isolationLevel = dxrConfig.isolationLevel

    val connectionsDF = getConnectionsDfFromDxrBaseConfig(
      config = dxrConfig,
      None,
      sqlNames,
      sqlStringsMap,
      isolationLevel,
      dxrConfig.upperBound,
      dxrConfig.lowerBound,
      dxrSql = dxrJobLog.DXR_SQL).drop("FINAL_SQL", "FINAL_SESSION_SETUP_CALL")

    val failedTargetsDF = sparkSession.read
      .jdbc(config.dxrJdbcUrl, config.jobLogsSql, config.dxrConnectionProperties)
      .withColumnRenamed("DB_PARTITION", "DB_SCHEMA")
      .withColumnRenamed("FINAL_SELECT_SQL", "FINAL_SQL")
      //      .withColumn("FINAL_SQL", concat(lit("("), $"FINAL_SELECT_SQL", lit(")")))
      .withColumn(
        "FINAL_SESSION_SETUP_CALL",
        when($"FINAL_SESSION_SETUP_CALL".isNotNull, $"FINAL_SESSION_SETUP_CALL")
          .otherwise(lit("")))
      .withColumn("TARGET_KEY", $"TARGET_KEY".cast(IntegerType))
      .withColumn("BATCH_NUMBER", $"BATCH_NUMBER".cast(IntegerType))
      .select(
        "SQL_NAME",
        "TARGET_KEY",
        "DB_SCHEMA",
        "BATCH_NUMBER",
        "FINAL_SESSION_SETUP_CALL",
        "FINAL_SQL")

    val failedTargetsDFWithOutLineage =
      sparkSession.createDataFrame(
        failedTargetsDF.collect().toSeq,
        failedTargetsDF.schema)

    failedTargetsDFWithOutLineage.explain()

    val failedConnectionDS = failedTargetsDFWithOutLineage
      .join(
        connectionsDF,
        Seq("SQL_NAME", "TARGET_KEY", "DB_SCHEMA", "BATCH_NUMBER"),
        "inner")
      .as[DXRConnection]

    failedConnectionDS
  }

  def recoverFailedWrites(
      config: dxrStagingDataProcessorConfig,
      dxrLogger: DXRLogger,
      dxrJobLog: DXRJobLog,
      dxrConfig: DxrParallelDataExtractorConfig)(implicit
      sparkSession: SparkSession): String = {

    val (rowCountsMap, corruptSqlsList) = if (config.skipRowCountValidation) {
      logger.warn(s"SKIP_VALIDATION: corrupt data check is being skipped")
      val countMap = dxrConfig.sqlFileNames.map(_ -> 0.toLong).toMap
      (countMap, List[String]())
    } else {
      val countMap = dxrLogger
        .getTaskSummaryFromApplicationId(config.applicationId)
        .collect()
        .toList
        .map(row => {
          (
            row.getAs[String]("SQL_NAME"),
            row.getAs[java.math.BigDecimal]("ROW_COUNT").longValue)
        })
        .toMap
      val corruptSqlsRegex = s"(?s).*CORRUPT_DATA_SQLS=(.*?)\\s.*".r
      val corruptSqls: List[String] =
        if (dxrJobLog.MSG.matches(corruptSqlsRegex.regex)) {
          val corruptSqlsRegex(corruptSqlNames) = dxrJobLog.MSG
          logger.warn(
            s"found the following CORRUPT_DATA_SQLS=$corruptSqlNames these will be skipped while writing")
          corruptSqlNames.split(",").toList
        } else List[String]()

      (countMap, corruptSqls)
    }

    val attemptId = dxrJobLog.ATTEMPT_ID.getOrElse("1")

    // read the folders that were left out from the last extract
    val pathString =
      s"/tmp/${sparkSession.sparkContext.sparkUser}/dxr2/${config.applicationId}/$attemptId/"
    val path = new Path(pathString)
    val pathsMaps: Map[String, String] = dxrParallelDataExtractor
      .getDXRFileSystem(dxrConfig)
      .listStatus(path)
      .toList
      .map(_.getPath.getName)
      .map(x => (x, pathString + x))
      .toMap

    val filteredPathsMap = pathsMaps.filterKeys(!corruptSqlsList.contains(_))

    val writeFailures = dxrDataWriter.writeToHiveTables(
      config                   = dxrConfig,
      pathsMap                 = filteredPathsMap,
      rowCountsMap             = rowCountsMap,
      addonDistributionColumns = config.addonDistributionColumns,
      dxrLogger                = dxrLogger,
      skipStagingCleanup       = config.skipStagingCleanup,
      isOverWrite              = true,
      skipRowCountValidation   = config.skipRowCountValidation)

    val failedWrites = writeFailures.mkString(",")
    failedWrites
  }

  def getFailedJobConfig(config: dxrStagingDataProcessorConfig)(implicit
      sparkSession: SparkSession): (DXRJobLog, DxrParallelDataExtractorConfig) = {

    import sparkSession.implicits._

    val dxrConnectionProperties = config.dxrConnectionProperties
    val dxrConfigDS = sparkSession.read
      .jdbc(config.dxrJdbcUrl, config.jobConfigSql, dxrConnectionProperties)
      .withColumn("is_error", col("is_error").cast(BooleanType))
      .as[DXRJobLog]

    require(
      !dxrConfigDS.isEmpty,
      s"no failures found from the previous execution plz check the details of ${config.applicationId}")

    dxrConfigDS.show(false)

    val dxrJobLog = dxrConfigDS.head()
    // replace old param names with new for backward compatibility
    val cfgJSON = dxrJobLog.JOB_CONFIG.trim
      .replaceFirst("outputDatabaseName", "greenDatabaseName")

    // fix for deserialization, transient variables are not serialized but need a place holder during deserialization, also empty place holders for backward compatibility
    val placeHolders =
      List(
        ("dxrDatabasePassword", "\"\""),
        ("dxrParmas", "{}"),
        ("skipStagingCleanup", false))

    val configJSONString = placeHolders.foldLeft(cfgJSON)((cfgString, nextPair) => {
      if (cfgString.contains(nextPair._1)) {
        cfgString
      } else {
        val dummyPlaceHolder = s"""{"${nextPair._1}":${nextPair._2},""".trim
        val replacedJSON =
          cfgString.trim.replaceFirst("^\\{", dummyPlaceHolder).trim
        replacedJSON
      }
    })

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val dxrConfig                             = read[DxrParallelDataExtractorConfig](configJSONString)

    if (config.recoverMode) {
      (dxrJobLog, dxrConfig)
    } else {
      // rerun mode should always append data overwriting will lead to data loss so force overwrite to false
      logger.warn(
        "DXR_RERUN: forcing table overwrite mode to false for rerun on failed connections")
      (dxrJobLog, dxrConfig.copy(isOverWrite = false))

    }
  }
}
