package com.adp.datacloud.ds.nosql

import com.adp.datacloud.cli.{DxrNosqlDataExtractorConfig, dxrNosqlDataExtractorOptions}
import com.adp.datacloud.ds.nosql.dxrNoSqlDataExtractor.getConnectionsDfFromConfig
import com.adp.datacloud.ds.util.DXRLogger
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DxrNoSqlDataExtractorTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw")
    setUpDatabase("testblueraw")
    setUpDatabase("testbluelanding")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testbluelanding")
  }

  lazy val extractConfig = {
    val resourcePrefix: String = s"/dxrSqls/mongo/"
    val sqlFileName            = s"mongo_extract"
    val sqlFilePath            = getPathByResourceName(sqlFileName + ".sql", resourcePrefix)
    sparkSession.sparkContext.addFile(sqlFilePath)

    val argsString = s"""
                         |--product-name MYADP_MONGO_FIT
                         |--dxr-jdbc-url $dxrJdbcURL
                         |--landing-db testbluelanding
                         |--sql-files $sqlFileName
                         |--sql-conf "__RO_GREEN_MAIN_DB__=test"
                         |--output-partition-spec yyyymm,db_schema
                         |--oracle-wallet-location $oracleWalletPath
                         |--application-name dxrDocumentDBExtractor
                         |--read-option readPreference=secondary
                         |--sql-conf env=DIT
                         |""".stripMargin.trim

    println(s"INPUT_ARGS:$argsString")
    val testArgs = argsString.split("\\s+")
    dxrNosqlDataExtractorOptions.parse(testArgs)
  }

  lazy val extractAutoIncrementalConfig   = buildTestIncrementalConfig(true)
  lazy val extractStaticIncrementalConfig = buildTestIncrementalConfig()

  def buildTestIncrementalConfig(
      autoIncrementalMode: Boolean = false): DxrNosqlDataExtractorConfig = {
    val resourcePrefix: String = s"/dxrSqls/mongo/"
    val sqlFileName            = s"mongo_extract"
    val sqlFilePath            = getPathByResourceName(sqlFileName + ".sql", resourcePrefix)
    sparkSession.sparkContext.addFile(sqlFilePath)

    val incrementalArgsString = if (autoIncrementalMode) {
      s""" --overwrite false"""
    } else {
      s"""
      --sql-conf INCREMENTAL_START_DATE=20210101
      --sql-conf INCREMENTAL_END_DATE=20210331
      """
    }

    val argsString = (s"""
                      |--product-name MYADP_MONGO_FIT
                      |--dxr-jdbc-url $dxrJdbcURL
                      |--landing-db testbluelanding
                      |--sql-files $sqlFileName
                      |--sql-conf "__RO_GREEN_MAIN_DB__=test"
                      |--output-partition-spec yyyymm,db_schema
                      |--oracle-wallet-location $oracleWalletPath
                      |--application-name dxrDocumentDBExtractor
                      |--read-option readPreference=secondary
                      |--managed-incremental true
                      |--sql-conf env=DIT
                      |""" + incrementalArgsString).stripMargin.trim

    println(s"INPUT_ARGS:$argsString")

    val testArgs = argsString.split("\\s+")

    dxrNosqlDataExtractorOptions.parse(testArgs)
  }

  test("should extract data from mongoDB") {
    printSparkConfAsJson()
    dxrNoSqlDataExtractor.processJob(extractConfig)
  }

  test("should do static incremental extract from mongoDB") {
    printSparkConfAsJson()
    dxrNoSqlDataExtractor.processJob(extractStaticIncrementalConfig)
  }

  test("should do auto incremental extract from mongoDB") {
    printSparkConfAsJson()
    dxrNoSqlDataExtractor.processJob(extractAutoIncrementalConfig)
  }

  test("extract collection names from sql and replace with views") {

    for (sqlName <- extractConfig.sqlFileNames) {
      val sqlString = extractConfig.getSqlFromFile(sqlName)

      val (replacedSql, replaceMap) = dxrNoSqlDataExtractor.getReplacedSql(sqlString)

      assert(
        dxrNoSqlDataExtractor.noSqlTableNameRegex.findAllMatchIn(replacedSql).isEmpty,
        "all files have not been replaced")

      assert(
        replaceMap.values.toList.contains("userAccount"),
        "nosql table name was not identified from sql")
    }

  }

  test("should create connections DS") {

    val connDS = getConnectionsDfFromConfig(extractConfig, None)
    connDS.show()
    connDS
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(s"${extractConfig.landingDatabaseName}.dxr_connections")

    val dxrLogger = DXRLogger(
      extractConfig,
      extractConfig.sqlFileNames,
      extractConfig.inputParams.getOrElse("env", ""))

    val staticDatesDS =
      dxrLogger.getIncrementalDateBySqlNames(
        extractStaticIncrementalConfig,
        extractConfig.sqlFileNames)

    val staticConnDS =
      getConnectionsDfFromConfig(extractStaticIncrementalConfig, staticDatesDS)
    staticConnDS.show()
    staticConnDS
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(
        s"${extractStaticIncrementalConfig.landingDatabaseName}.dxr_static_incremental_connections")

    val dynamicDatesDS =
      dxrLogger.getIncrementalDateBySqlNames(
        extractStaticIncrementalConfig,
        extractConfig.sqlFileNames)

    val dynamicConnDS =
      getConnectionsDfFromConfig(extractStaticIncrementalConfig, dynamicDatesDS)
    dynamicConnDS.show()
    dynamicConnDS
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", true)
      .saveAsTable(
        s"${extractStaticIncrementalConfig.landingDatabaseName}.dxr_dynamic_incremental_connections")
  }
}
