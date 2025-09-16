package com.adp.datacloud.ds.util

import com.adp.datacloud.cli.DxrBaseConfig
import com.adp.datacloud.ds.hive.hiveTableUtils
import com.adp.datacloud.ds.util.DXRLoggerHelper._
import com.adp.datacloud.writers.DataCloudDataFrameWriterLog
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.elasticsearch.spark.sql._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}
import scala.collection.JavaConverters.propertiesAsScalaMapConverter

class DXRLogger(
    url: String,
    props: Properties,
    applicationName: String,
    productName: String,
    dxrSql: String,
    inputSqls: List[String],
    jobConfigJson: String,
    envName: String,
    esCredentialsConfMap: Map[String, String],
    parentApplicationId: String = null,
    skipLogsToDelta: Boolean    = true)(implicit sparkSession: SparkSession) {

  private val logger = Logger.getLogger(getClass)
  val minTimestamp   = new Timestamp(0)

  /**
   * logs to delta table were introduced due to performance issues on dxr logs table on oracle.
   * after introducing stats collection and indices the performance issues are no more.
   * this make me wonder if this delta logging is even needed defaulting it to be disabled for the most past for now
   */
  val skipDeltaLogs: Boolean =
    skipLogsToDelta || (sparkSession.conf
      .getOption("spark.master")
      .getOrElse("") == "yarn")

  lazy val stateMachineExecutionId: String = sparkSession.conf
    .getOption("spark.orchestration.stateMachine.execution.id")
    .getOrElse("NA")
  lazy val workflowName: String = if (stateMachineExecutionId.split(":").length > 6) {
    stateMachineExecutionId.split(":")(6)
  } else {
    "NA"
  }
  lazy val clusterId: String =
    sparkSession.conf.getOption("spark.orchestration.cluster.id").getOrElse("NA")

  lazy implicit private val indexSuffix: String = {
    val sdf = new SimpleDateFormat("yyyyMM")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf.format(new Date())
  }

  /**
   * This method fetches date compatible to auto date detection in ES
   *
   * @see
   * [[https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#strict-date-time]]
   * [[https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html#date-detection]]
   * @return
   */
  def getCurrentESTimeStamp(date: Date = new Date()): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf.format(date)
  }

  {
    val loggingTables =
      List(
        extractStatusTable,
        extractLogsTable,
        exportLogsTable,
        hiveWriterLogsTable,
        extractIncrementBoundsTable,
        dxrExtractSummaryTable,
        extractLiveLogsTable)

    // Create the table if the table didn't exist.
    loggingTables.foreach(table => {
      val tableOptions = new JdbcOptionsInWrite(url, table, props.asScala.toMap)
      val conn         = JdbcUtils.createConnectionFactory(tableOptions)()
      try {
        val tableExists = JdbcUtils.tableExists(conn, tableOptions)
        if (!tableExists) {
          val sourceFile = scala.io.Source
            .fromInputStream(getClass.getResourceAsStream(s"/create_$table.sql"))
          val createTableSql =
            try sourceFile.mkString
            finally sourceFile.close()
          val statement = conn.createStatement
          try {
            statement.executeUpdate(createTableSql)
          } finally {
            statement.close()
          }
        }

      } finally {
        conn.close()
      }
    })
  }

  import sparkSession.implicits._

  val sc: SparkContext = sparkSession.sparkContext

  /**
   * returns a dataframe with start and enddate per sql fetched from its its previous execution details stored in spark_extractor_increment_bounds table.
   * if the input sql parameters contain variables INCREMENTAL_START_DATE and INCREMENTAL_END_DATE they take precedence over data fetched from table.
   * if INCREMENTAL_END_DATE is not provided then current_timestamp from dxr catalog db is considered
   *
   * @param config
   * @return
   */
  def getIncrementalDateBySqlNames(
      config: DxrBaseConfig,
      sqlFileNames: List[String]): Option[Dataset[DXRIncrementStatus]] = {
    import sparkSession.implicits._
    val applicationId = sc.applicationId

    require(envName.trim.nonEmpty, "env param value is required to fetch dates by param")

    val boundsTable = sparkSession.read
      .jdbc(
        config.dxrJdbcUrl,
        extractIncrementBoundsTable,
        config.dxrConnectionProperties)

    boundsTable.createTempView(extractIncrementBoundsTable)

    val selectSql = if (config.inputParams.contains("INCREMENTAL_START_DATE")) {
      val incrementalStartDate =
        s"to_timestamp('${config.inputParams("INCREMENTAL_START_DATE")}','yyyyMMdd')"
      val incrementalEndDate =
        if (config.inputParams.contains("INCREMENTAL_END_DATE"))
          s"to_timestamp('${config.inputParams("INCREMENTAL_END_DATE")}','yyyyMMdd')"
        else "current_timestamp()"

      s"""
         |SELECT
         |    '$applicationId' AS APPLICATION_ID,
         |    '$envName' AS ENV_NAME,
         |    explode(array(${sqlFileNames
        .mkString("'", "','", "'")})) as SQL_NAME,
         |    $incrementalStartDate AS INCREMENTAL_START_DATE,
         |    $incrementalEndDate AS INCREMENTAL_END_DATE
         |""".stripMargin.trim
    } else {
      s"""
         |SELECT
         |    '$applicationId' AS APPLICATION_ID,
         |    ENV_NAME,
         |    SQL_NAME,
         |    incremental_end_date as INCREMENTAL_START_DATE,
         |    current_timestamp() as INCREMENTAL_END_DATE
         |FROM
         |( SELECT
         |    env_name,
         |    sql_name,
         |    incremental_start_date,
         |    incremental_end_date,
         |    rank() OVER (PARTITION BY env_name,sql_name ORDER BY incremental_end_date DESC) as rank
         |FROM
         |    $extractIncrementBoundsTable
         |WHERE
         |    env_name = '$envName'
         |    AND sql_name IN (
         |    ${sqlFileNames.mkString("'", "','", "'")}
         |    ))
         | WHERE rank = 1
         |""".stripMargin.trim
    }

    logger.info(s"INCREMENTAL_BOUNDS_SQL:$selectSql")

    val incrementalBoundsDSWithLineage =
      sparkSession.sql(selectSql).as[DXRIncrementStatus]
    incrementalBoundsDSWithLineage.printSchema()

    val incrementalBoundsDS =
      if (incrementalBoundsDSWithLineage.isEmpty)
        None
      else {
        incrementalBoundsDSWithLineage.show(false)
        logger.info(s"logging lineage for incrementalBoundsDSWithLineage in stdout")
        incrementalBoundsDSWithLineage.explain()
        // remove lineage
        logger.info(s"removing lineage from  incrementalBoundsDSWithLineage")
        val incrementalBoundsDSWithOutLineage =
          incrementalBoundsDSWithLineage.collect().toList.toDS()
        logger.info(s"logging lineage for incrementalBoundsDSWithOutLineage in stdout")
        incrementalBoundsDSWithOutLineage.explain()
        Some(incrementalBoundsDSWithOutLineage)
      }
    sparkSession.catalog.dropTempView(extractIncrementBoundsTable)
    incrementalBoundsDS
  }

  def insertIncrementalDates(
      incrementalDatesDS: Dataset[DXRIncrementStatus],
      landingDatabaseName: Option[String] = None): Boolean = {
    incrementalDatesDS.show(false)
    if (landingDatabaseName.isDefined && !skipDeltaLogs) {
      insertIncrementalDatesToDeltaTable(incrementalDatesDS, landingDatabaseName.get)
    }
    writeLogsToJdbc(incrementalDatesDS.toDF(), extractIncrementBoundsTable)
  }

  //TODO: try to remove the overhead of creating a dataframe just to insert 1 record
  def insertStartStatusLog(
      landingDatabaseName: Option[String],
      message: String = null): Boolean = {
    val insertDS: Dataset[DXRJobLog] = List(
      DXRJobLog(
        APPLICATION_ID             = sc.applicationId,
        ATTEMPT_ID                 = sc.applicationAttemptId,
        APPLICATION_NAME           = applicationName,
        PRODUCT_NAME               = productName,
        DXR_SQL                    = dxrSql,
        INPUT_SQL                  = inputSqls.mkString(","),
        JOB_CONFIG                 = jobConfigJson,
        JOB_STATUS                 = "STARTED",
        JOB_STATUS_CODE            = 1,
        CLUSTER_ID                 = clusterId,
        WORKFLOW_NAME              = workflowName,
        STATE_MACHINE_EXECUTION_ID = stateMachineExecutionId,
        JOB_START_TIME             = new Timestamp(System.currentTimeMillis().toLong),
        JOB_END_TIME               = minTimestamp,
        ENV                        = envName,
        MSG                        = message,
        PARENT_APPLICATION_ID      = parentApplicationId)).toDS()

    if (landingDatabaseName.isDefined && !skipDeltaLogs) {
      insertStatusLogIntoDeltaTable(landingDatabaseName.get, insertDS)
    }
    writeLogsToJdbc(insertDS.toDF(), extractStatusTable)
  }

  def updateStatusLog(
      landingDatabaseName: Option[String],
      jobStatus: String,
      jobStatusCode: Int,
      msg: String,
      isError: Boolean = false): Boolean = {
    if (landingDatabaseName.isDefined && !skipDeltaLogs) {
      updateStatusLogInDeltaTable(
        landingDatabaseName.get,
        jobStatus,
        jobStatusCode,
        msg,
        isError)

    }
    updateJobStatusInJdbc(jobStatus, jobStatusCode, msg, isError)
  }

  def updateJobStatusInJdbc(
      jobStatus: String,
      jobStatusCode: Int,
      msg: String,
      isError: Boolean): Boolean = {
    val trimmedMsg = if (msg.length > 4000) {
      msg.substring(0, 4000)
    } else msg
    val updateSql =
      s"update $extractStatusTable set job_status='$jobStatus', job_status_code = $jobStatusCode, job_end_time = current_timestamp(6),MSG = TO_CLOB('$trimmedMsg'),is_error = ${if (isError) 1
      else 0} where application_id = '${sc.applicationId}'"
    try {
      executeUpdateInDxrCatalog(updateSql, extractStatusTable)
      true
    } catch {
      case e: Exception =>
        logger.error(s"updating status into $extractStatusTable failed:", e)
        false
    }
  }

  private def executeUpdateInDxrCatalog(inputSql: String, table: String): Unit = {
    val tableOptions = new JdbcOptionsInWrite(url, table, props.asScala.toMap)
    val conn         = JdbcUtils.createConnectionFactory(tableOptions)()
    try {
      val statement = conn.createStatement
      try {
        statement.executeUpdate(inputSql)
        statement.setQueryTimeout(120)
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  def calculateAndRecordExtractSummary(
      jobStartTime: Long,
      taskLogsDS: Option[Dataset[DXRTaskLog]],
      landingDatabaseName: String,
      corruptDataSqls: List[String] = List(),
      failedWrites: List[String]    = List(),
      applicationId: String         = sc.applicationId): Boolean = {

    try {

      val (summaryDF: DataFrame, deltaSummaryDf: DataFrame, esSummaryDf: DataFrame) =
        constructExtractSummaryDfs(
          jobStartTime        = jobStartTime,
          taskLogsDS          = taskLogsDS,
          landingDatabaseName = landingDatabaseName,
          corruptDataSqls     = corruptDataSqls,
          failedWrites        = failedWrites,
          applicationId       = applicationId)

      ingestLogsToES(esSummaryDf, dxrExtractSummaryESIndexName)
      if (!skipDeltaLogs)
        insertSummaryLogToDeltaLake(deltaSummaryDf, landingDatabaseName)
      writeLogsToJdbc(summaryDF, dxrExtractSummaryTable)
      true
    } catch {
      case e: Exception =>
        logger.error("failed writing summary logs", e)
        false
    }

  }

  def constructExtractSummaryDfs(
      jobStartTime: Long,
      taskLogsDS: Option[Dataset[DXRTaskLog]],
      landingDatabaseName: String,
      corruptDataSqls: List[String] = List(),
      failedWrites: List[String]    = List(),
      applicationId: String         = sc.applicationId): (DataFrame, DataFrame, DataFrame) = {
    val taskDS = if (taskLogsDS.isDefined) {
      taskLogsDS.get
    } else {
      sparkSession.read
        .jdbc(
          url,
          s"(select * from $extractLogsTable WHERE application_id = '$applicationId')",
          props)
        .withColumn("TARGET_KEY", $"TARGET_KEY".cast(IntegerType))
        .withColumn("EXEC_TIME_MS", $"EXEC_TIME_MS".cast(IntegerType))
        .withColumn("ROW_COUNT", $"EXEC_TIME_MS".cast(IntegerType))
        .withColumn("EXTRACT_ATTEMPT_ID", $"EXEC_TIME_MS".cast(IntegerType))
        .withColumn("TASK_ATTEMPT_NUMBER", $"TASK_ATTEMPT_NUMBER".cast(IntegerType))
        .withColumn("TASK_ATTEMPT_ID", $"TASK_ATTEMPT_ID".cast(IntegerType))
        .withColumn("BATCH_NUMBER", $"BATCH_NUMBER".cast(LongType))
        .withColumn("IS_ERROR", $"IS_ERROR".cast(BooleanType))
        .as[DXRTaskLog]
    }

    taskDS.createTempView("TASKS_LOGS_TEMP_TABLE")
    val summarySql =
      s"""
         |SELECT
         |    APPLICATION_ID,
         |    SQL_NAME,
         |    COUNT(TARGET_KEY) AS TARGET_COUNT,
         |    SUM(ROW_COUNT) AS ROW_COUNT,
         |    SUM(
         |        CASE
         |            WHEN IS_ERROR = 0   THEN 1
         |            ELSE 0
         |        END
         |    ) AS CONNECTIONS_SUCCESS_COUNT,
         |    SUM(
         |        CASE
         |            WHEN IS_ERROR = 1   THEN 1
         |            ELSE 0
         |        END
         |    ) AS CONNECTIONS_ERROR_COUNT,
         |    ROUND(SUM(EXEC_TIME_MS) / 1000,2) AS TOTAL_SEC_DB_TIME,
         |    ROUND(AVG(EXEC_TIME_MS) / 1000,2) AS AVG_SEC,
         |    ROUND(MAX(EXEC_TIME_MS) / 1000,2) AS MAX_EXEC_SEC,
         |    ROUND((PERCENTILE(EXEC_TIME_MS,0.80)) / 1000,2) PCTL_80TH_SEC,
         |    ROUND((PERCENTILE(EXEC_TIME_MS,0.90)) / 1000,2) PCTL_90TH_SEC,
         |    ROUND((PERCENTILE(EXEC_TIME_MS,0.95)) / 1000,2) PCTL_95TH_SEC,
         |    ROUND((PERCENTILE(EXEC_TIME_MS,0.99)) / 1000,2) PCTL_99TH_SEC,
         |    ROUND(AVG(ROW_COUNT),0) AS AVG_ROW_COUNT,
         |    MAX(ROW_COUNT) AS MAX_ROW_COUNT,
         |    ROUND(PERCENTILE(EXEC_TIME_MS,0.80),0) AS PCTL_80TH_ROW_COUNT,
         |    ROUND(PERCENTILE(EXEC_TIME_MS,0.90),0) AS PCTL_90TH_ROW_COUNT,
         |    ROUND(PERCENTILE(EXEC_TIME_MS,0.95),0) AS PCTL_95TH_ROW_COUNT,
         |    ROUND(PERCENTILE(EXEC_TIME_MS,0.99),0) AS PCTL_99TH_ROW_COUNT
         |FROM
         |    TASKS_LOGS_TEMP_TABLE
         |GROUP BY
         |    SQL_NAME, APPLICATION_ID
         |CLUSTER BY
         |    SQL_NAME, APPLICATION_ID
         |""".stripMargin.trim

    val aggDf = sparkSession.sql(summarySql)
    sparkSession.catalog.dropTempView("TASKS_LOGS_TEMP_TABLE")

    val corruptsSqlsDf = inputSqls
      .map(x => (x, if (corruptDataSqls.contains(x)) 1 else 0))
      .toDF("SQL_NAME", "IS_DATA_CORRUPT")
    val failedWritesDf =
      inputSqls
        .map(x => (x, if (failedWrites.contains(x)) 1 else 0))
        .toDF("SQL_NAME", "IS_FAILED_WRITE")

    val summaryDF = aggDf
      .join(corruptsSqlsDf, usingColumn = "SQL_NAME")
      .join(failedWritesDf, usingColumn = "SQL_NAME")
      .withColumn(
        "IS_FAILURE",
        when($"IS_DATA_CORRUPT" === 1 || $"IS_FAILED_WRITE" === 1, 1).otherwise(0))

    val deltaSummaryDf = summaryDF
      .withColumn("CLUSTER_ID", lit(clusterId))
      .withColumn("WORKFLOW_NAME", lit(workflowName))
      .withColumn("STATE_MACHINE_EXECUTION_ID", lit(stateMachineExecutionId))
      .withColumn("JOB_START_TIME", to_timestamp(lit(jobStartTime / 1000)))
      .withColumn("JOB_END_TIME", current_timestamp())
      .withColumn("ENV", lit(envName))

    // convert dates to es format
    val esSummaryDf = deltaSummaryDf
      .withColumn(
        "JOB_START_TIME",
        date_format($"JOB_START_TIME", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
      .withColumn(
        "JOB_END_TIME",
        date_format($"JOB_END_TIME", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

    (summaryDF, deltaSummaryDf, esSummaryDf)
  }

  def getTaskSummaryFromApplicationId(applicationId: String): DataFrame = {
    sparkSession.read
      .jdbc(
        url,
        s"(select * from $dxrExtractSummaryTable WHERE application_id = '$applicationId')",
        props)
  }

  // TODO: find a better approach to insert a single record
  def writeTasksLog(taskLog: DXRTaskLog): Boolean = {
    writeTasksLogs(List(taskLog).toDS())
  }

  def writeTasksLogs(
      logsDS: Dataset[DXRTaskLog],
      landingDatabaseName: Option[String] = None): Boolean = {
    if (landingDatabaseName.isDefined && !skipDeltaLogs) {
      writeTasksLogsToDeltaTable(logsDS, landingDatabaseName.get)
    }
    writeLogsToJdbc(logsDS.toDF(), extractLogsTable)
  }

  def writeExportTasksLogs(logsDF: DataFrame): Boolean = {
    ingestLogsToES(logsDF, exportLogsESIndexName) & writeLogsToJdbc(
      logsDF,
      exportLogsTable)
  }

  //TODO: try to remove the overhead of creating a dataframe just to insert 1 record
  def insertHiveDfWriterLog(log: DataCloudDataFrameWriterLog): Boolean = {
    val logsDF = List(log).toDF()
    ingestLogsToES(logsDF, hiveWriterLogsIndexName) & writeLogsToJdbc(
      logsDF,
      hiveWriterLogsTable)
  }

  def writeLogsToJdbc(logsDF: DataFrame, tableName: String): Boolean = {
    try {
      val logTableOptions = new JdbcOptionsInWrite(url, tableName, props.asScala.toMap)
      JdbcUtils.saveTable(logsDF, None, isCaseSensitive = false, logTableOptions)
      true
    } catch {
      case e: Exception =>
        logger.error(s"Error while writing logs to $tableName", e)
        e.printStackTrace()
        false
    }
  }

  // TODO: try to make is async/ or set timeout so that it doesn't slow down the main process
  def ingestLogsToES(df: DataFrame, indexName: String)(implicit
      suffix: String): Boolean = {
    val rollingIndexName = indexName + "-" + suffix
    val ts               = System.currentTimeMillis()
    try {
      val logDf = df.columns
        .foldRight(df)({ (x, y) =>
          y.withColumnRenamed(x, x.toLowerCase())
        })
        .withColumn("log_timestamp", lit(getCurrentESTimeStamp()))
        .withColumn("cluster_id", lit(clusterId))
        .withColumn("state_machine_execution_id", lit(stateMachineExecutionId))

      // don't wait for log more than 15 seconds
      logDf.saveToEs(rollingIndexName, esCredentialsConfMap.+("es.http.timeout" -> "15s"))
      true
    } catch {
      case e: Exception =>
        val execTime = System.currentTimeMillis() - ts
        logger.error(
          s"failed to ingest logs in to Elastic Search Index: $rollingIndexName executionTime: $execTime errorMsg: ${e.getMessage}")
        false
    }
  }

  // logging into delta lake - handle errors along with exceptions
  private def insertStatusLogIntoDeltaTable(
      landingDatabaseName: String,
      statusDS: Dataset[DXRJobLog]): Boolean = {

    val jobStatusDeltaTableName = s"$landingDatabaseName.dxr_job_status"
    val statusDf                = statusDS.toDF()

    try {

      val finalDf =
        statusDf.columns.foldRight(statusDf)({ (x, y) =>
          y.withColumnRenamed(x, x.toLowerCase())
        })

      finalDf.write
        .format("delta")
        .mode(SaveMode.Append)
        .option("mergeSchema", "true")
        .partitionBy("product_name")
        .saveAsTable(jobStatusDeltaTableName)
      true
    } catch {
      case e: Exception =>
        logger.error(s"failed inserting status into $jobStatusDeltaTableName:", e)
        false
      case err: Error =>
        logger.error(s"failed inserting status into $jobStatusDeltaTableName:", err)
        false
    }
  }

  private def updateStatusLogInDeltaTable(
      landingDatabaseName: String,
      status: String,
      statusCode: Integer,
      msg: String,
      isError: Boolean = false): Boolean = {
    val jobStatusDeltaTableName = s"$landingDatabaseName.dxr_job_status"
    try {
      import io.delta.tables._
      val tableLocation    = hiveTableUtils.getTableLocation(jobStatusDeltaTableName)
      val statusDeltaTable = DeltaTable.forPath(tableLocation)
      val applicationId    = sc.applicationId

      statusDeltaTable.update(
        $"application_id" === applicationId,
        Map(
          "job_status" -> lit(status),
          "job_status_code" -> lit(statusCode).cast(IntegerType),
          "msg" -> lit(msg),
          "is_error" -> lit(isError).cast(BooleanType),
          "job_end_time" -> current_timestamp()))
      true
    } catch {
      case e: Exception =>
        logger.error(s"failed updating status into $jobStatusDeltaTableName:", e)
        false
      case err: Error =>
        logger.error(s"failed updating status into $jobStatusDeltaTableName:", err)
        false
    }
  }

  private def insertIncrementalDatesToDeltaTable(
      incrementalDatesDS: Dataset[DXRIncrementStatus],
      landingDatabaseName: String): Boolean = {

    val jobStatusDeltaTableName =
      s"$landingDatabaseName.dxr_extractor_increment_bounds"

    try {
      incrementalDatesDS.write
        .format("delta")
        .mode(SaveMode.Append)
        .option("mergeSchema", "true")
        .partitionBy("sql_name")
        .saveAsTable(jobStatusDeltaTableName)
      true
    } catch {
      case e: Exception =>
        logger.error(s"failed inserting status into $jobStatusDeltaTableName", e)
        false
      case err: Error =>
        logger.error(s"failed updating status into $jobStatusDeltaTableName:", err)
        false
    }
  }

  private def writeTasksLogsToDeltaTable(
      ds: Dataset[DXRTaskLog],
      landingDatabaseName: String): Boolean = {
    val logTableName = s"$landingDatabaseName.dxr_extractor_logs"
    try {
      ds.write
        .format("delta")
        .mode(SaveMode.Append)
        .option("mergeSchema", "true")
        .partitionBy("sql_name")
        .saveAsTable(logTableName)
      true
    } catch {
      case e: Exception =>
        logger.error(s"Error while writing logs to $logTableName", e)
        e.printStackTrace()
        false
      case err: Error =>
        logger.error(s"Error while writing logs to $logTableName", err)
        false
    }

  }

  private def insertSummaryLogToDeltaLake(
      summaryDf: DataFrame,
      landingDatabaseName: String): Boolean = {
    val logTableName = s"$landingDatabaseName.$dxrExtractSummaryTable"
    try {
      summaryDf.write
        .format("delta")
        .mode(SaveMode.Append)
        .option("mergeSchema", "true")
        .partitionBy("sql_name")
        .saveAsTable(logTableName)
      true
    } catch {
      case e: Exception =>
        logger.error(s"Error while writing logs to $logTableName:", e)
        e.printStackTrace()
        false
      case err: Error =>
        logger.error(s"Error while writing logs to $logTableName:", err)
        false
    }
  }

}

object DXRLogger {

  def apply(
      config: DxrBaseConfig,
      inputSqls: List[String],
      envName: String             = "",
      parentApplicationId: String = null)(implicit
      sparkSession: SparkSession): DXRLogger = {

    val connectionProps = config.dxrConnectionProperties
    // avoid hung up stmts, set timeout for 2 mins
    connectionProps.setProperty("queryTimeout", "120")

    new DXRLogger(
      config.dxrJdbcUrl,
      connectionProps,
      config.applicationName,
      config.dxrProductName,
      config.dxrSql,
      inputSqls,
      config.toJson,
      envName,
      config.getESCredentialsConfMap(sparkSession.conf.getAll),
      parentApplicationId)
  }
}
