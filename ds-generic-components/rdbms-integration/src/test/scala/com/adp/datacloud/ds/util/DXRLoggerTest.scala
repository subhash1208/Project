package com.adp.datacloud.ds.util

import com.adp.datacloud.cli.{DxrBaseConfig, DxrParallelDataExtractorConfig, dxrParallelDataExtractorOptions}
import com.adp.datacloud.ds.rdbms.dxrFunctions.logExtractStatus
import com.adp.datacloud.ds.util.DXRLoggerHelper.extractLiveLogsTable
import com.adp.datacloud.util.SparkTestWrapper
import com.adp.datacloud.writers.DataCloudDataFrameWriterLog
import org.apache.spark.SparkFiles
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DXRLoggerTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  // uncomment the below line to use local ES for testing
  //  override val useLocalES = true

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw", clean    = true)
    setUpDatabase("testblueraw", clean     = true)
    setUpDatabase("testbluelanding", clean = true)
  }

  case class TestDxrConfig(
      dxrProductName: String            = "TEST",
      dxrJdbcUrl: String                = dxrJdbcURL,
      applicationName: String           = "DXRLoggerUtilTest",
      oracleWalletLocation: String      = oracleWalletPath,
      inputParams: Map[String, String]  = Map(),
      dxrProductVersion: Option[String] = None,
      jdbcDBType: String                = "oracle",
      esUsername: Option[String]        = Some("ds_ingest"),
      esPwd: Option[String]             = None,
      files: Option[String]             = None)
      extends DxrBaseConfig {}

  val baseConfig = TestDxrConfig()

  printSparkConfAsJson()

  val dxrLogger = DXRLogger(TestDxrConfig(), List("DXRLoggerUtilTestSQL"), "TEST")

  import sparkSession.implicits._

  val indexSuffix = "test-index"

  val hdfwLogDf = List(
    DataCloudDataFrameWriterLog(
      sparkSession.sparkContext.applicationId,
      sparkSession.sparkContext.applicationAttemptId,
      "DXRLoggerUtilTest",
      0,
      1,
      1,
      1)).toDF

  lazy val dxrConfig: DxrParallelDataExtractorConfig = {
    val dxrArgs = buildTestDxrArgs(s"dxr_target_query.sql")
    val config  = dxrParallelDataExtractorOptions.parse(dxrArgs)
    config
  }

  private def registerLogTablesAsView(
      dxrConfig: DxrBaseConfig,
      tableName: String): Unit = {

    val df: DataFrame = sparkSession.read.jdbc(
      dxrConfig.dxrJdbcUrl,
      s"ADPI_DXR.$tableName",
      dxrConfig.dxrConnectionProperties)
    df.createOrReplaceTempView(tableName)

  }

  private def buildTestDxrArgs(
      dxrProductName: String,
      resourcePrefix: String = s"/dxrSqls/oracle/"): Array[String] = {
    if (dxrProductName.endsWith(".sql")) {
      val dxrTargetQuerySQLPath = getPathByResourceName(dxrProductName, resourcePrefix)
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
      s"""--product-name $dxrProductName
      --dxr-jdbc-url $dxrJdbcURL
      --output-partition-spec db_schema
      --sql-files ${extractNames.mkString(",")}
      --strict-mode true
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master none
      --oracle-wallet-location $oracleWalletPath
      --sql-conf HISTORY_MONTHS_START=201901
      --sql-conf HISTORY_MONTHS_END=201812
      """.stripMargin.split("\\s+")
    dxrArgs
  }

  test("insertHiveDfWriterLog to rdbms") {
    assert(dxrLogger.writeLogsToJdbc(hdfwLogDf, DXRLoggerHelper.hiveWriterLogsTable))
  }

  test("insertHiveDfWriterLog to ES") {
    assert(
      dxrLogger.ingestLogsToES(hdfwLogDf, DXRLoggerHelper.hiveWriterLogsIndexName)(
        indexSuffix))
  }

  test("writeExportTasksLogs to rdbms") {}

  test("writeExportTasksLogs to ES") {}

  test("writeTasksLogs to rdbms") {}

  test("writeTasksLogs to ES") {}

  test("insertStatusLog to rdbms") {}

  test("insertStatusLog to ES") {}

  test("summary calculation") {
    val dataPath = getPathByResourceName("dxrTaskLogs.snappy.parquet")
    val logsDF = sparkSession.read
      .parquet(dataPath)
      .withColumn("TARGET_KEY", $"TARGET_KEY".cast(IntegerType))
    logsDF.show(false)
    assert(dxrLogger.writeTasksLogs(logsDF.as[DXRTaskLog]))
  }

  test("insert dxr liveTaskLog") {

    val taskContext = org.apache.spark.TaskContext.get()
    val (taskAttemptNumber: Int, taskAttemptId: Long) = if (taskContext != null) {
      (taskContext.attemptNumber(), taskContext.taskAttemptId())
    } else (0, 0.toLong)



    val dxrLog = DXRTaskLog(
      APPLICATION_ID           = sparkSession.sparkContext.applicationId,
      ATTEMPT_ID               = sparkSession.sparkContext.applicationAttemptId,
      SQL_NAME                 = "dummy",
      TARGET_KEY               = 12345,
      DB_PARTITION             = "dummy",
      FINAL_SELECT_SQL         = "dummy",
      FINAL_SESSION_SETUP_CALL = "",
      MSG                      = "test insert",
      TASK_ATTEMPT_NUMBER      = taskAttemptNumber,
      TASK_ATTEMPT_ID          = taskAttemptId)

    logExtractStatus(dxrLog, baseConfig.dxrConnectionProperties, dxrJdbcURL)

    registerLogTablesAsView(baseConfig, extractLiveLogsTable)

    val logsDf = sparkSession
      .table(extractLiveLogsTable)
      .where(s"application_id=='${sparkSession.sparkContext.applicationId}'")

    assert(!logsDf.isEmpty, "log not recorded")
    logsDf.show(false)

  }

}
