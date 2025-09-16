package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{
  DxrBaseConfig,
  DxrParallelDataExtractorConfig,
  dxrParallelDataExtractorOptions,
  dxrStagingDataProcessorConfig,
  dxrStagingDataProcessorOptions
}
import com.adp.datacloud.ds.util.{DXR2Exception, DXRJobLog, DXRLogger, DXRLoggerHelper}
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.log4j.Logger
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.functions.{col, lit, when}
import org.json4s.jackson.Serialization.writePretty
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito.spy
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.io.{File, PrintWriter, StringWriter}
import java.sql.{Connection, Statement}
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.reflect.io.Directory
import scala.sys.process._

@RunWith(classOf[JUnitRunner])
class DxrStagingDataProcessorTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  val logger = Logger.getLogger(getClass)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cleanAndSetupDatabase("testgreenraw")
    cleanAndSetupDatabase("testblueraw")
    cleanAndSetupDatabase("testbluelanding")
    createTableFromResource("testbluelanding", s"optout_master", s"optout_master")
  }

  // TODO: get this scenario fixed
  lazy val s3StagingRerunConfig: dxrStagingDataProcessorConfig = {
    updateJobStatus(s"local-1614857434780", true)
    val dxrRerunArgs = buildTestDxrRerunArgs(s"local-1614857434780")
//    val dxrRerunArgs = buildTestDxrRerunArgs(s"application_1623463797124_0006")
    //    val dxrRerunArgs = buildTestDxrRerunArgs(s"local-1617185995988")
    val config = dxrStagingDataProcessorOptions.parse(dxrRerunArgs)
    config
  }

  lazy val localStagingRerunConfig: dxrStagingDataProcessorConfig = {
    val applicationId = s"local-1617185995988"
    updateJobStatus(applicationId, false)
    val stagingPath = s"/tmp/${sparkSession.sparkContext.sparkUser}/dxr2/$applicationId"
    val directory   = new Directory(new File(stagingPath))
    if (directory.exists)
      directory.deleteRecursively()
    s"mkdir -p $stagingPath".!

    val dataPath = getPathByResourceName("staging_files")
    val cpCmd =
      s"cp -R $dataPath/$applicationId/ $stagingPath"
    println(s"Running Copy command $cpCmd ..")
    cpCmd.!
    val dxrRerunArgs = buildTestDxrRerunArgs(applicationId)
    val config       = dxrStagingDataProcessorOptions.parse(dxrRerunArgs)
    config
  }

  test("parse rerun args into config") {
    println(writePretty(s3StagingRerunConfig))
  }

  test("rerun successfully from local staging dir") {
    dxrStagingDataProcessor.processFailedJob(localStagingRerunConfig)
  }

  test("rerun successfully from s3 staging dir") {
    dxrStagingDataProcessor.processFailedJob(s3StagingRerunConfig)
  }

  ignore("insert custom status log") {

    val resourcePrefix: String = s"/dxrSqls/oracle/"
    val productName            = s"dxr_target_query.sql"
    if (productName.endsWith(".sql")) {
      val dxrTargetQuerySQLPath = getPathByResourceName(productName, resourcePrefix)
      sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)
    }
    val extractNames = List("extract_test", "extract_tab_test")
    val extractFilePaths =
      extractNames.map(name => getPathByResourceName(s"$name.sql", resourcePrefix))

    // add files to sparkContext
    extractFilePaths.foreach(sparkSession.sparkContext.addFile)
    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))
    val dxrArgs =
      s"""--product-name $productName
      --dxr-jdbc-url $dxrJdbcURL
      --s3-staging-bucket $codeStoreBucketAWSParam
      --oracle-wallet-location $oracleWalletPath
      --output-partition-spec db_schema
      --sql-files ${extractNames.mkString(",")}
      --strict-mode true
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master none
      --sql-conf HISTORY_MONTHS_START=201901
      --sql-conf HISTORY_MONTHS_END=201812
      """.stripMargin.split("\\s+")

    val dxrConfig = dxrParallelDataExtractorOptions.parse(dxrArgs)

    val dxrLogger = DXRLogger(
      dxrConfig,
      dxrConfig.sqlFileNames,
      dxrConfig.inputParams.getOrElse("env", ""))
    dxrLogger.insertStartStatusLog(None)

    val extractWallClockTime  = 0
    val writeWallClockTime    = 0
    val connectionErrorsCount = 0
    val zeroCountSqls         = ""
    val failedWrites          = "extract_test"

    val statusMsgString =
      s"EXTRACT_WALL_CLOCK_TIME_MS=$extractWallClockTime,WRITE_WALL_CLOCK_TIME_MS=$writeWallClockTime,CONNECTION_ERROR_COUNT=$connectionErrorsCount,ZERO_RECORDS_SQLS=$zeroCountSqls,FAILED_WRITE_SQLS=$failedWrites"

    try {
      throw DXR2Exception(statusMsgString)
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        dxrLogger.updateStatusLog(None, "FAILED", 4, sw.toString, isError = true)
    }

    val appId = sparkSession.sparkContext.applicationId
    println(s"Logs created for $appId")
    appId
  }

  def updateJobStatus(applicationId: String, s3Staging: Boolean): Unit = {

    val jdbcOptions = new JDBCOptions(
      dxrJdbcURL,
      "SPARK_EXTRACTOR_JOB_STATUS",
      DxrBaseConfig
        .getDxrConnectionPropertiesByWalletLocation(oracleWalletPath)
        .asScala
        .toMap)

    val updateSql = if (s3Staging) {

      s"""
        |UPDATE spark_extractor_job_status
        |SET    job_config = To_clob(
        |'{"dxrProductName":"dxr_target_query.sql","dxrJdbcUrl":"$dxrJdbcURL","jdbcDBType":"oracle","inputParams":{"HISTORY_MONTHS_START":"201901","HISTORY_MONTHS_END":"201812"},"outputPartitionSpec":"db_schema","applicationName":"DXR Parallel Data Extractor","sqlFileNames":["extract_test","extract_tab_test"],"maxParallelConnectionsPerDb":10,"fetchArraySize":1000,"numBatches":4,"upperBound":200,"lowerBound":0,"limit":0,"singleTargetOnly":false,"skipStagingCleanup":false,"queryTimeout":1800,"nullStrings":["UNKNOWN","NULL","unknown","null"],"oracleWalletLocation":"/app/oraclewallet","s3StagingBucket":"$codeStoreBucketAWSParam","isolationLevel":2,"strictMode":true,"compressionCodec":"SNAPPY","greenDatabaseName":"testgreenraw","blueDatabaseName":"testblueraw","landingDatabaseName":"testbluelanding"}'
        |)
        |WHERE  application_id = '$applicationId'
        |""".stripMargin.trim

    } else {
      s"""
        |UPDATE spark_extractor_job_status
        |SET    job_config = To_clob(
        |'{"dxrProductName":"dxr_target_query.sql","dxrJdbcUrl":"$dxrJdbcURL","jdbcDBType":"oracle","inputParams":{"HISTORY_MONTHS_START":"201901","HISTORY_MONTHS_END":"201812"},"outputPartitionSpec":"db_schema","applicationName":"DXR Parallel Data Extractor","sqlFileNames":["extract_test","extract_tab_test"],"maxParallelConnectionsPerDb":10,"fetchArraySize":1000,"numBatches":4,"upperBound":200,"lowerBound":0,"limit":0,"singleTargetOnly":false,"skipStagingCleanup":false,"queryTimeout":1800,"nullStrings":["UNKNOWN","NULL","unknown","null"],"oracleWalletLocation":"/app/oraclewallet","isolationLevel":2,"strictMode":true,"compressionCodec":"SNAPPY","greenDatabaseName":"testgreenraw","blueDatabaseName":"testblueraw","landingDatabaseName":"testbluelanding"}'
        |)
        |WHERE  application_id = '$applicationId'
        |""".stripMargin.trim
    }

    var connection: Connection = null
    var stmt: Statement        = null
    try {
      connection = JdbcUtils.createConnectionFactory(jdbcOptions)()
      stmt       = connection.createStatement()
      stmt.executeUpdate(updateSql)
    } catch {
      case e: Exception => logger.error("", e)
    } finally {
      if (connection != null)
        connection.close()
      if (stmt != null)
        stmt.close()
    }

  }

  /**
   * tests for recovery by rerunning failed connections
   */

  lazy val failedConnectionsRerunConfig: dxrStagingDataProcessorConfig = {
    val dxrRerunArgs = buildTestDxrRerunArgs(s"dxrRerunTest20211108-1", "False")
    val config       = dxrStagingDataProcessorOptions.parse(dxrRerunArgs)
    config
  }

  test("process a failed job by rerunning") {

    val dxrStagingDataProcessorSpy = spy(dxrStagingDataProcessor)
    val (dxrJobLog: DXRJobLog, dxrConfig: DxrParallelDataExtractorConfig) =
      dxrStagingDataProcessor.getFailedJobConfig(failedConnectionsRerunConfig)

    Mockito
      .when(dxrStagingDataProcessorSpy.getFailedJobConfig(failedConnectionsRerunConfig))
      .thenReturn((dxrJobLog, patchDxrConfig(dxrConfig)))

    try {
      dxrStagingDataProcessorSpy.processFailedJob(failedConnectionsRerunConfig)

    } catch {
      case de: DXR2Exception => {
        validateDxrWithPartialError(de.getMessage)
      }
      case e: Exception => throw e
    }
  }

  test("rerun a failed dxr run") {
    val (dxrJobLog: DXRJobLog, dxrConfig: DxrParallelDataExtractorConfig) =
      dxrStagingDataProcessor.getFailedJobConfig(failedConnectionsRerunConfig)
    val dxrLogger =
      DXRLogger(
        failedConnectionsRerunConfig,
        List[String](),
        failedConnectionsRerunConfig.inputParams.getOrElse("env", "test"),
        failedConnectionsRerunConfig.applicationId)
    try {
      val logsDS = dxrStagingDataProcessor.rerunFailedConnections(
        dxrLogger,
        failedConnectionsRerunConfig,
        dxrJobLog,
        patchDxrConfig(dxrConfig))
      assert(logsDS.filter(_.IS_ERROR).isEmpty)
    } catch {
      case de: DXR2Exception => validateDxrWithPartialError(de.getMessage)
      case e: Exception      => throw e
    }
  }

  test("construct failed connections df") {
    val (dxrJobLog: DXRJobLog, dxrConfig: DxrParallelDataExtractorConfig) =
      dxrStagingDataProcessor.getFailedJobConfig(failedConnectionsRerunConfig)
    val connectionsDS = dxrStagingDataProcessor.getFailedConnectionsDS(
      failedConnectionsRerunConfig,
      dxrJobLog,
      patchDxrConfig(dxrConfig))

    assert(!connectionsDS.isEmpty)
    connectionsDS.show(false)
  }

  private def validateDxrWithPartialError(
      dxrMsg: String,
      applicationId: String = sparkSession.sparkContext.applicationId) = {
    val errorsMap = getMapFromDxrMsg(dxrMsg)
    assert(!errorsMap.contains("FAILED_WRITE_SQLS"))
    // TODO: validate this better, check if there were any write failures
    // check if latest some of the queries were successfully i.e partial connection failures are accepted
    assert(
      !sparkSession.read
        .jdbc(
          failedConnectionsRerunConfig.dxrJdbcUrl,
          s"(select * from ${DXRLoggerHelper.extractLogsTable} where application_id = '$applicationId' and is_error = 0)",
          failedConnectionsRerunConfig.dxrConnectionProperties)
        .isEmpty,
      dxrMsg)
  }

  private def getMapFromDxrMsg(dxrMsg: String): Map[String, String] = {
    dxrMsg
      .split(",")
      .flatMap(x => {
        val tokens = x.split("=")
        if (tokens.size > 1 && tokens(1).trim.nonEmpty) {
          Some(tokens(0) -> tokens(1))
        } else None
      })
      .toMap
  }

  private def patchDxrConfig(
      dxrConfig: DxrParallelDataExtractorConfig): DxrParallelDataExtractorConfig = {
    dxrConfig
      .copy(oracleWalletLocation = oracleWalletPath)
      .copy(greenDatabaseName = Some("testgreenraw"))
      .copy(blueDatabaseName = Some("testblueraw"))
      .copy(landingDatabaseName = "testbluelanding")
      .copy(s3StagingBucket = None)
  }

  ignore("generate dummy data for rerun testing..") {
    // batching example application_1635951889870_0001 -->  dxrRerunTest20211108-1
    //  application_1635945155204_0003  -->  dxrRerunTest20211110-1
    //  app-20211104124207-0000  -->  dxrRerunTest20211111-1
    val sourceAppId        = s"app-20211104124207-0000"
    val dxrRerunArgs       = buildTestDxrRerunArgs(sourceAppId)
    val config             = dxrStagingDataProcessorOptions.parse(dxrRerunArgs)
    val dummyApplicationId = "dxrRerunTest20211111-1"
    val statusDf = sparkSession.read
      .jdbc(
        config.dxrJdbcUrl,
        s"(select * from ${DXRLoggerHelper.extractStatusTable} where APPLICATION_ID = '$sourceAppId')",
        config.dxrConnectionProperties)
      .withColumn("APPLICATION_ID", lit(dummyApplicationId))
    val logsDf = sparkSession.read
      .jdbc(
        config.dxrJdbcUrl,
        s"(select * from ${DXRLoggerHelper.extractLogsTable} where APPLICATION_ID='$sourceAppId')",
        config.dxrConnectionProperties)
      .withColumn("APPLICATION_ID", lit(dummyApplicationId))
      .withColumn(
        "IS_ERROR",
        col("EXEC_TIME_MS") % 2
      ) // force some of them to be failures

    statusDf.write
      .mode(SaveMode.Append)
      .jdbc(
        config.dxrJdbcUrl,
        DXRLoggerHelper.extractStatusTable,
        config.dxrConnectionProperties)
    logsDf.write
      .mode(SaveMode.Append)
      .jdbc(
        config.dxrJdbcUrl,
        DXRLoggerHelper.extractLogsTable,
        config.dxrConnectionProperties)
  }

  private def buildTestDxrRerunArgs(
      applicationId: String,
      recoveryMode: String = "True"): Array[String] = {
    val dxrRerunArgs =
      s"""--dxr-jdbc-url $dxrJdbcURL
        --application-id $applicationId
        --oracle-wallet-location $oracleWalletPath
        --application-name dxr_recovery
        --skip-staging-cleanup False
        --skip-rowcount-validation True
        --recovery-mode $recoveryMode
        --es-username ds_ingest
        --addon-distribution-columns ""
      """.stripMargin.split("\\s+")
    dxrRerunArgs
  }
}
