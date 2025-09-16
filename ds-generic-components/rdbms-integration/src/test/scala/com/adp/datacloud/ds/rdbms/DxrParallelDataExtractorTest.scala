package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{DxrParallelDataExtractorConfig, dxrParallelDataExtractorOptions}
import com.adp.datacloud.ds.rdbms.dxrParallelDataExtractor.{distributeConnectionsByDBLimit, getConnectionsDfFromConfig}
import com.adp.datacloud.ds.util.{DXR2Exception, DXRConnection, DXRLogger}
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.json4s.jackson.Serialization.writePretty
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class DxrParallelDataExtractorTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw",true)
    setUpDatabase("testblueraw",true)
    setUpDatabase("testbluelanding",true)
    createTableFromResource("testbluelanding", s"optout_master", s"optout_master")
  }

  // uncomment the below line to use localES for testing
  //  override val useLocalES: Boolean = true

  lazy val dxrConfig: DxrParallelDataExtractorConfig = {
    val dxrArgs = buildTestDxrArgs(s"dxr_target_query.sql")
    val config  = dxrParallelDataExtractorOptions.parse(dxrArgs)
    config
  }

  lazy val mysqlDxrConfig: DxrParallelDataExtractorConfig = {
    val dxrArgs = buildMySqlTestDxrArgs(s"dxr_sbs_target_query.sql")
    val config  = dxrParallelDataExtractorOptions.parse(dxrArgs)
    config
  }

  lazy val redShiftDxrConfig: DxrParallelDataExtractorConfig = {
    val dxrArgs = buildReadShiftTestArgs(s"dxr_redshift_target_query.sql")
    val config  = dxrParallelDataExtractorOptions.parse(dxrArgs)
    config
  }

  private def buildTestDxrArgs(
                                dxrProductName: String,
                                resourcePrefix: String = s"/dxrSqls/oracle/"): Array[String] = {

    if (dxrProductName.endsWith(".sql")) {
      val dxrTargetQuerySQLPath =
        getPathByResourceName(dxrProductName, resourcePrefix)
      sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)
    }

    val extractNames = List("extract_test", "extract_tab_test")
    //    val extractNames     = List("extract_test")
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
      --files ${extractFilePaths.mkString(",")}
      --strict-mode true
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master none
      --maintenance-check true
      --hash-columns sys_date
      --oracle-wallet-location $oracleWalletPath
      --sql-conf HISTORY_MONTHS_START=201901
      --sql-conf HISTORY_MONTHS_END=201812
      --sql-conf env=DIT
      """.stripMargin.split("\\s+")
    dxrArgs
  }

  private def buildMySqlTestDxrArgs(
                                     dxrProductName: String,
                                     resourcePrefix: String = s"/dxrSqls/mysql/"): Array[String] = {
    if (dxrProductName.endsWith(".sql")) {
      val dxrTargetQuerySQLPath = getPathByResourceName(dxrProductName, resourcePrefix)
      sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)
    }

    //    val extractNames     = List("sbs_source_databases","run_person")
    val extractNames = List("run_person")
    val extractFilePaths =
      extractNames.map(name => getPathByResourceName(s"$name.sql", resourcePrefix))

    // add files to sparkContext
    extractFilePaths.foreach(sparkSession.sparkContext.addFile)
    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    val dxrArgs =
      s"""--product-name $dxrProductName
      --dxr-jdbc-url $dxrJdbcURL
      --output-partition-spec environment
      --sql-files ${extractNames.mkString(",")}
      --files ${extractFilePaths.mkString(",")}
      --strict-mode true
      --source-db sqlserver
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master testbluelanding.optout_master
      --maintenance-check false
      --batch-column iid
      --oracle-wallet-location $oracleWalletPath
      --sql-conf ntile_val=15
      --sql-conf NTILE_VAL=15
      --sql-conf env=DIT
      --sql-conf ENV=DIT
      --sql-conf INGEST_YYYYMMDD=20210330
      --sql-conf END_YYYYMMDD=20210330
      --sql-conf PARTITION_YYYYMMDD=20210330
      --sql-conf ingest_month=202102
      --sql-conf INGEST_MONTH=202102
      --sql-conf yyyymm=202102
      --sql-conf YYYYMM=202102
      --sql-conf yyyymm_lower=202102
      --sql-conf YYYYMM_LOWER=202102
      --sql-conf yyyymm_upper=202102
      --sql-conf YYYYMM_UPPER=202102
      --sql-conf yyyymmww=2021W12
      --sql-conf YYYYMMWW=2021W12
      --sql-conf yyyymmww_lower=2021W12
      --sql-conf YYYYMMWW_LOWER=2021W12
      --sql-conf yyyymmww_upper=2021W12
      --sql-conf YYYYMMWW_UPPER=2021W12
      --sql-conf yyyymmdd_startofweek=20210321
      --sql-conf YYYYMMDD_STARTOFWEEK=20210321
      --sql-conf yyyymmdd_endofweek=20210327
      --sql-conf YYYYMMDD_ENDOFWEEK=20210327
      --sql-conf yyyymmdd=20210330
      --sql-conf YYYYMMDD=20210330
      --sql-conf yyyymmdd_lower=20210330
      --sql-conf YYYYMMDD_LOWER=20210330
      --sql-conf yyyymmdd_upper=20210330
      --sql-conf YYYYMMDD_UPPER=20210330
      --sql-conf qtr=2020Q4
      --sql-conf QTR=2020Q4
      --sql-conf qtr_lower=2020Q4
      --sql-conf QTR_LOWER=2020Q4
      --sql-conf qtr_upper=2020Q4
      --sql-conf QTR_UPPER=2020Q4
      --sql-conf next_qtr_first_day=20210101
      --sql-conf NEXT_QTR_FIRST_DAY=20210101
      --sql-conf current_qtr_last_day=20201231
      --sql-conf CURRENT_QTR_LAST_DAY=20201231
      --sql-conf yyyy=2021
      --sql-conf YYYY=2021
      --skip-staging-cleanup true
      """.stripMargin.split("\\s+")
    dxrArgs
  }

  def buildReadShiftTestArgs(
                              dxrProductName: String,
                              resourcePrefix: String =
                              s"${File.separator}dxrSqls${File.separator}redshift${File.separator}")
  : Array[String] = {
    if (dxrProductName.endsWith(".sql")) {
      val dxrTargetQuerySQLPath =
        getPathByResourceName(dxrProductName, resourcePrefix)
      sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)
    }

    val extractNames = List("t_dim_job")
    //    val extractNames     = List("extract_test")
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
      --files ${extractFilePaths.mkString(",")}
      --strict-mode true
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master none
      --source-db redshift
      --oracle-wallet-location $oracleWalletPath
      --sql-conf HISTORY_MONTHS_START=201901
      --sql-conf HISTORY_MONTHS_END=201812
      --sql-conf env=DIT
      """.stripMargin.split("\\s+")
    dxrArgs

  }

  test("parse dxrExtract args into config") {
    println(writePretty(dxrConfig))
  }

  test("connections should be distributed by max connections per db") {

    import sparkSession.implicits._

    val wfnFitDxrConfig =
      dxrParallelDataExtractorOptions.parse(
        buildTestDxrArgs("fit_wfn_dr_target_query.sql"))

    val connFinalDS = dxrParallelDataExtractor.getConnectionsDfFromConfig(wfnFitDxrConfig)

    val dbCount = connFinalDS.map(_.DB_ID).distinct().count()

    val distributedConnectionsDS: Dataset[DXRConnection] =
      distributeConnectionsByDBLimit(wfnFitDxrConfig, connFinalDS, Some(dbCount))

    // assert total partitions generated
    assert(
      distributedConnectionsDS.rdd.getNumPartitions == (dbCount * wfnFitDxrConfig.maxParallelConnectionsPerDb))

    // extract counts of a given db from all partitions
    val filteredList = distributedConnectionsDS
      .mapPartitions((partition: Iterator[DXRConnection]) => {
        if (partition.hasNext) {
          List(partition.filter(_.DB_ID == 1).size).toIterator
        } else {
          Iterator.empty
        }
      })
      .collect()
      .toList
      .filter(_ > 0)

    // check if a given db connections don't exist in partitions beyond max connections per db
    assert(filteredList.size <= wfnFitDxrConfig.maxParallelConnectionsPerDb)
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

    sparkSession
      .table(s"${dxrConfig.landingDatabaseName}.extract_test")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(s"${dxrConfig.landingDatabaseName}.extract_testText")
  }

  test("run dxr extract successfully on mysql target with static batching") {
    printSparkConfAsJson()
    try {
      dxrParallelDataExtractor.runExtract(mysqlDxrConfig)
    } catch {
      case e: DXR2Exception => e.printStackTrace()
    }
  }

  test("run dxr extract successfully on redshift target") {
    printSparkConfAsJson()
    try {
      dxrParallelDataExtractor.runExtract(redShiftDxrConfig)
    } catch {
      case e: DXR2Exception => e.printStackTrace()
    }
  }

  test("should create table testgreenraw.extract_test ") {
    val dataDf = sparkSession.table("testgreenraw.extract_test")
    dataDf
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("testgreenraw.export_test")
  }

  test("should create connections DS") {

    val dxrLogger = DXRLogger(
      dxrConfig,
      dxrConfig.sqlFileNames,
      dxrConfig.inputParams.getOrElse("env", ""))
    val datesDS =
      dxrLogger.getIncrementalDateBySqlNames(dxrConfig, dxrConfig.sqlFileNames)

    val connDS = getConnectionsDfFromConfig(dxrConfig, datesDS)
    connDS.show()
    connDS
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(s"${dxrConfig.landingDatabaseName}.dxr_connections")

    val mysqlConnDS = getConnectionsDfFromConfig(mysqlDxrConfig)
    mysqlConnDS.show()
    mysqlConnDS
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(s"${dxrConfig.landingDatabaseName}.dxr_mysql_connections")
  }

}