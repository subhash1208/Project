package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{
  DxrParallelDataExtractorConfig,
  dxrParallelDataExtractorOptions
}
import com.adp.datacloud.ds.rdbms.dxrParallelDataExtractor.getConnectionsDfFromConfig
import com.adp.datacloud.ds.util.{DXR2Exception, DXRLogger}
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.io.{PrintWriter, StringWriter}

@RunWith(classOf[JUnitRunner])
class DxrParallelDataExtractorIncrementalTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw", clean    = true)
    setUpDatabase("testblueraw", clean     = true)
    setUpDatabase("testbluelanding", clean = true)
    createTableFromResource("testbluelanding", s"optout_master", s"optout_master")
  }

  lazy val dxrConfig: DxrParallelDataExtractorConfig = {
    val dxrArgs = buildTestDxrArgs()
    val config  = dxrParallelDataExtractorOptions.parse(dxrArgs)
    config
  }

  lazy val incrementalDxrConfig: DxrParallelDataExtractorConfig = {
    val dxrArgs = buildTestDxrArgs(true)
    val config  = dxrParallelDataExtractorOptions.parse(dxrArgs)
    config
  }

  private def buildTestDxrArgs(
      incrementalMode: Boolean = false,
      resourcePrefix: String   = s"/dxrSqls/oracle/"): Array[String] = {
    val dxrProductName = s"dxr_target_query.sql"

    val dxrTargetQuerySQLPath =
      getPathByResourceName(s"$dxrProductName", resourcePrefix)
    sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)

    val extractNames = List("wfn_t_empl_login", "wfn_t_empl_prfl", "wfn_t_mobl_accs")
    val extractFilePaths =
      extractNames.map(name => getPathByResourceName(s"$name.sql", resourcePrefix))

    sparkSession.conf.set("spark.orchestration.cluster.id", "j-2AJ6WYHUAZSHZ")
    sparkSession.conf.set(
      "spark.orchestration.stateMachine.execution.id",
      "arn:aws:states:us-east-1:708035784431:execution:APP-DXR-DATA-ASSETS-DXR_CONNECTIVITY_CHECK:cb560f5a-4a7c-fe89-ca32-a8b1c4781506")

    // add files to sparkContext
    extractFilePaths.foreach(sparkSession.sparkContext.addFile)
    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    val incrementalArgsString = if (incrementalMode) {
      s""" --overwrite false"""
    } else {
      s"""
      --sql-conf INCREMENTAL_START_DATE=20210101
      --sql-conf INCREMENTAL_END_DATE=20210331
      """
    }

    val dxrArgsString = s"""--product-name $dxrProductName
      --dxr-jdbc-url $dxrJdbcURL
      --output-partition-spec db_schema
      --sql-files ${extractNames.mkString(",")}
      --strict-mode false
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master testbluelanding.optout_master
      --retry-interval-secs 55
      --managed-incremental true
      --sql-conf env=DIT
      --oracle-wallet-location $oracleWalletPath""" + incrementalArgsString

    val dxrArgs = dxrArgsString.stripMargin.split("\\s+")
    dxrArgs
  }

  ignore("insert and update status logs to delta table") {

    val dxrLogger = DXRLogger(
      dxrConfig,
      dxrConfig.sqlFileNames,
      dxrConfig.inputParams.getOrElse("env", ""))
    dxrLogger.insertStartStatusLog(Some(dxrConfig.landingDatabaseName))

    val logsDF = sparkSession.table(s"${dxrConfig.landingDatabaseName}.dxr_job_status")
    logsDF.show()
    logsDF.printSchema()

    val statusMsgString =
      s"EXTRACT_WALL_CLOCK_TIME_MS=101,WRITE_WALL_CLOCK_TIME_MS=102,CONNECTION_ERROR_COUNT=103,ZERO_RECORDS_SQLS=wfn_t_mobl_accs,FAILED_WRITE_SQLS="

    try {
      throw DXR2Exception(statusMsgString)
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        dxrLogger.updateStatusLog(
          Some(dxrConfig.landingDatabaseName),
          "FAILED",
          4,
          sw.toString,
          isError = true)
      }
    }

    val updatedLogsDF =
      sparkSession.table(s"${dxrConfig.landingDatabaseName}.dxr_job_status")
    updatedLogsDF.show()
    updatedLogsDF.printSchema()
  }

  ignore("read and write incremental dates with overridden params") {
    val dxrLogger = DXRLogger(
      dxrConfig,
      dxrConfig.sqlFileNames,
      dxrConfig.inputParams.getOrElse("env", ""))
    val incrementalDates =
      dxrLogger.getIncrementalDateBySqlNames(dxrConfig, dxrConfig.sqlFileNames)
    val datesDS = incrementalDates.get
    datesDS.show()
    dxrLogger.insertIncrementalDates(datesDS)
  }

  ignore("read and write incremental dates from table") {
    val dxrLogger =
      DXRLogger(
        incrementalDxrConfig,
        incrementalDxrConfig.sqlFileNames,
        incrementalDxrConfig.inputParams.getOrElse("env", ""))
    val incrementalDates = dxrLogger.getIncrementalDateBySqlNames(
      incrementalDxrConfig,
      incrementalDxrConfig.sqlFileNames)
    val datesDS = incrementalDates.get
    datesDS.show()
    dxrLogger.insertIncrementalDates(datesDS)
  }

  test("should create connections DS") {
    val dxrLogger = DXRLogger(
      dxrConfig,
      dxrConfig.sqlFileNames,
      dxrConfig.inputParams.getOrElse("env", ""))
    val incrementalDates =
      dxrLogger.getIncrementalDateBySqlNames(dxrConfig, dxrConfig.sqlFileNames)
    val datesDS = incrementalDates.get
    datesDS.show()
    assert(datesDS.where("incremental_start_date is null").isEmpty)
    assert(datesDS.where("incremental_end_date is null").isEmpty)
    val connDS = getConnectionsDfFromConfig(dxrConfig, incrementalDates)
    connDS.show()
    connDS.select("FINAL_SQL").show(10, false)
    connDS
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(s"${dxrConfig.landingDatabaseName}.dxr_connections")
  }

  test("run dxr extract successfully") {
    printSparkConfAsJson()
    try {
      dxrParallelDataExtractor.runExtract(dxrConfig)
    } catch {
      case e: DXR2Exception => e.printStackTrace()
    }
    displayAllTables(dxrConfig.landingDatabaseName)
    if (dxrConfig.blueDatabaseName.isDefined) {
      displayAllTables(dxrConfig.blueDatabaseName.get)
    }
    if (dxrConfig.greenDatabaseName.isDefined) {
      displayAllTables(dxrConfig.greenDatabaseName.get)
    }

//    displayDeltaTableLogs()

    incrementalDxrConfig.sqlFileNames.foreach(sqlName => {
      if (sparkSession.catalog.tableExists(
          s"${incrementalDxrConfig.landingDatabaseName}.$sqlName")) {
        sparkSession
          .table(s"${incrementalDxrConfig.landingDatabaseName}.$sqlName")
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("csv")
          .option("header", true)
          .saveAsTable(s"${incrementalDxrConfig.landingDatabaseName}.${sqlName}_csv")
      }
    })
  }

  test("should create incremental connections DS") {
    val dxrLogger =
      DXRLogger(
        incrementalDxrConfig,
        incrementalDxrConfig.sqlFileNames,
        incrementalDxrConfig.inputParams.getOrElse("env", ""))
    val incrementalDates = dxrLogger.getIncrementalDateBySqlNames(
      incrementalDxrConfig,
      incrementalDxrConfig.sqlFileNames)
    incrementalDates.get.show()
    assert(incrementalDates.get.where("incremental_end_date is null").isEmpty)
    val connDS = getConnectionsDfFromConfig(incrementalDxrConfig, incrementalDates)
    connDS.show()
    connDS.select("FINAL_SQL").show(10, false)
    connDS
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(
        s"${incrementalDxrConfig.landingDatabaseName}.dxr_incremental_connections")
  }

  test("run dxr incremental extract successfully") {
    printSparkConfAsJson()
    try {
      dxrParallelDataExtractor.runExtract(incrementalDxrConfig)
    } catch {
      case e: DXR2Exception => e.printStackTrace()
    }
    displayAllTables(incrementalDxrConfig.landingDatabaseName)
    if (incrementalDxrConfig.blueDatabaseName.isDefined) {
      displayAllTables(incrementalDxrConfig.blueDatabaseName.get)
    }
    if (incrementalDxrConfig.greenDatabaseName.isDefined) {
      displayAllTables(incrementalDxrConfig.greenDatabaseName.get)
    }

//    displayDeltaTableLogs()

    incrementalDxrConfig.sqlFileNames.foreach(sqlName => {
      if (sparkSession.catalog.tableExists(
          s"${incrementalDxrConfig.landingDatabaseName}.$sqlName")) {
        sparkSession
          .table(s"${incrementalDxrConfig.landingDatabaseName}.$sqlName")
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("csv")
          .option("header", true)
          .saveAsTable(
            s"${incrementalDxrConfig.landingDatabaseName}.${sqlName}_incremental")
      }
    })

  }

  test("get summary from applicationId") {

    val dxrLogger =
      DXRLogger(
        incrementalDxrConfig,
        incrementalDxrConfig.sqlFileNames,
        incrementalDxrConfig.inputParams.getOrElse("env", ""))
    val applicationId = "local-1623852648314"
    val (summaryDF: DataFrame, deltaSummaryDf: DataFrame, esSummaryDf: DataFrame) =
      dxrLogger.constructExtractSummaryDfs(
        jobStartTime        = System.currentTimeMillis(),
        taskLogsDS          = None,
        landingDatabaseName = "",
        applicationId       = applicationId)

    summaryDF.show(false)
    summaryDF.write
      .mode(SaveMode.Append)
      .jdbc(
        incrementalDxrConfig.dxrJdbcUrl,
        "summary_test",
        incrementalDxrConfig.dxrConnectionProperties)
  }

  def displayDeltaTableLogs(landingDatabaseName: String = "testbluelanding") = {
    val applicationId = sparkSession.sparkContext.applicationId

    sparkSession
      .table(s"${landingDatabaseName}.dxr_job_status")
      .where(s"application_id='$applicationId'")
      .show(true)
    sparkSession
      .table(s"${landingDatabaseName}.dxr_extractor_increment_bounds")
      .where(s"application_id='$applicationId'")
      .show(false)
    sparkSession
      .table(s"${landingDatabaseName}.dxr_extractor_logs")
      .where(s"application_id='$applicationId'")
      .show(true)
    sparkSession
      .table(s"${landingDatabaseName}.dxr_extractor_sql_summary")
      .where(s"application_id='$applicationId'")
      .show(false)

  }
}
