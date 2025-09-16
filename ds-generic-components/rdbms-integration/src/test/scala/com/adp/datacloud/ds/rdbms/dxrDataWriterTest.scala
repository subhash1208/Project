package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{
  DxrParallelDataExtractorConfig,
  dxrParallelDataExtractorOptions
}
import com.adp.datacloud.ds.util.DXRLogger
import com.adp.datacloud.util.SparkTestWrapper
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

case class WriteTestRow(clnt_obj_id: String, test_stricthash: String, test_hash: String)

@RunWith(classOf[JUnitRunner])
class dxrDataWriterTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  val stagingDirectoryPath  = s"$baseResourceAbsolutePath/spark-warehouse/dxrStaging"
  val outputTableName       = "dxr_write_test"
  val optoutTableName       = "optout_master"
  lazy val landingTableName = s"${extractConfig.landingDatabaseName}.$outputTableName"
  lazy val bluedbTableName  = s"${extractConfig.blueDatabaseName.get}.$outputTableName"
  lazy val greenTableName   = s"${extractConfig.greenDatabaseName.get}.$outputTableName"

  override def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw")
    setUpDatabase("testblueraw")
    setUpDatabase("testbluelanding")
    createTableFromResource("testbluelanding", s"$optoutTableName", s"$optoutTableName")
    // create data in staging location
    sparkSession.read
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .csv(getPathByResourceName("clnt_obj_id.csv"))
      .withColumn("test_stricthash", lit("STRICT_HASH_ME"))
      .withColumn("test_hash", lit("JUST_HASH_ME"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$stagingDirectoryPath/$outputTableName/")
  }

  lazy val extractConfig: DxrParallelDataExtractorConfig = {
    val dxrArgs = buildTestDxrArgs(s"dxr_target_query.sql")
    dxrParallelDataExtractorOptions.parse(dxrArgs)
  }

  lazy val dxrLogger: DXRLogger =
    DXRLogger(
      extractConfig,
      extractConfig.sqlFileNames,
      extractConfig.inputParams.getOrElse("env", ""))

  test("write data from staging successfully") {
    HiveDataFrameWriter("parquet", extractConfig.outputPartitionSpec)
    dxrDataWriter.writeToHiveTables(
      extractConfig,
      Map(s"$outputTableName" -> s"$stagingDirectoryPath/$outputTableName/"),
      Map(s"$outputTableName" -> 100),
      None,
      dxrLogger,
      extractConfig.skipStagingCleanup,
      skipRowCountValidation = true,
      isOverWrite            = true)
    validateOptOutAndHashing
  }

  test("opt out and hashing should be valid w.r.t to database") {
    validateOptOutAndHashing
  }

  private def buildTestDxrArgs(
      dxrProductName: String,
      resourcePrefix: String = s"/dxrSqls/oracle/"): Array[String] = {
    val productName = dxrProductName
    if (dxrProductName.endsWith(".sql")) {
      val dxrTargetQuerySQLPath = getPathByResourceName(productName, resourcePrefix)
      sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)
    }

    val dxrArgs =
      s"""--product-name $productName
      --dxr-jdbc-url $dxrJdbcURL
      --output-partition-spec product_code
      --sql-files $outputTableName
      --strict-mode true
      --skip-staging-cleanup true
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master testbluelanding.${optoutTableName}
      --oracle-wallet-location $oracleWalletPath
      --sql-conf HISTORY_MONTHS_START=201901
      --sql-conf HISTORY_MONTHS_END=201812
      """.stripMargin.split("\\s+")
    dxrArgs
  }

  def validateOptOutAndHashing(): Unit = {

    import sparkSession.implicits._
    val stagingdf = sparkSession.read.parquet(s"$stagingDirectoryPath/$outputTableName/")

    val optoutdf = sparkSession.table(s"testbluelanding.$optoutTableName")
    val joinedf  = optoutdf.join(stagingdf, "clnt_obj_id")
    assert(
      !joinedf.isEmpty,
      "cannot validate optout as there is no common data to filter")

    val optoutClientsList = joinedf
      .select("clnt_obj_id")
      .distinct()
      .collect()
      .map(_.getAs[String]("clnt_obj_id"))
    val randomIndex = System.currentTimeMillis() % optoutClientsList.length

    val randomClientId: String = optoutClientsList(randomIndex.toInt)

    val selectColumns =
      classOf[WriteTestRow].getDeclaredFields.map(field => col(field.getName))

    val landingTable: Dataset[WriteTestRow] = sparkSession
      .table(landingTableName)
      .select(selectColumns: _*)
      .as[WriteTestRow]
    val blueTable = sparkSession
      .table(bluedbTableName)
      .select(selectColumns: _*)
      .as[WriteTestRow]
    val greenTable = sparkSession
      .table(greenTableName)
      .select(selectColumns: _*)
      .as[WriteTestRow]

    // check optout

    assert(
      !landingTable.filter($"clnt_obj_id" === randomClientId).isEmpty,
      "optout shouldn't apply on landing")

    assert(
      blueTable.filter($"clnt_obj_id" === randomClientId).isEmpty,
      "blue db shouldn't have optout clients")

    assert(!blueTable.isEmpty, "cannot verify the test if blue db data is empty")

    assert(
      landingTable.count() > blueTable.count(),
      "landing db data should have more rows than blue db")

    assert(
      greenTable.filter($"clnt_obj_id" === randomClientId).isEmpty,
      "green db table shouldn't have optout clients in it")

    assert(
      greenTable.count() === blueTable.count(),
      "green db data should have equal rows as blue db")

    // check hashing

    assert(
      !landingTable
        .head()
        .test_stricthash
        .equalsIgnoreCase("STRICT_HASH_ME"),
      "Strict hashing invalid in landingdb")

    assert(
      landingTable
        .head()
        .test_hash
        .equals("JUST_HASH_ME"),
      "plain hashing invalid in landingdb")

    assert(
      !blueTable.head.test_stricthash
        .equalsIgnoreCase("STRICT_HASH_ME"),
      "Strict hashing invalid in bluedb")
    assert(
      blueTable.head.test_hash
        .equals("JUST_HASH_ME"),
      "plain hashing invalid in bluedb")

    assert(
      !greenTable.head.test_stricthash
        .equalsIgnoreCase("STRICT_HASH_ME"),
      "Strict hashing invalid in greendb")

    assert(
      !greenTable.head.test_hash
        .equals("JUST_HASH_ME"),
      "plain hashing invalid in greendb")

  }

}
