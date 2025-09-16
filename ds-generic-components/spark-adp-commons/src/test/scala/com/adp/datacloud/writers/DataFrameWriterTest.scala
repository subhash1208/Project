package com.adp.datacloud.writers

import com.adp.datacloud.util.SparkTestWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataFrameWriterTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  private val logger = Logger.getLogger(getClass())

  override def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenraw")
  }

  val mix           = "cola=AAA,yr_cd"
  val static        = "yr_cd=2017,qtr_cd=20171,mnth_cd=201703"
  val dynamic       = "yr_cd,qtr_cd,mnth_cd"
  val dynamic_wrong = "qtr_cd,mnth_cd,yr_cd"

  def PartitionTest(
      spec: String,
      tableName: String  = "testgreenraw.target",
      overWrite: Boolean = false) {
    println(s"Test for $spec partition spec")
    if (!overWrite) {
      sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
      sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    }
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    // Set qtr_cd null to make the computation tractable while running in local
    val hiveWriter = HiveDataFrameWriter("parquet", Some(spec))
    val partCols   = hiveWriter.partitionColumns.map(col)
    //    dataFrame.select(partCols: _*).distinct().show(false)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val optDf =
      sparkSession.sql(s"select * from $tableName").select(partCols: _*).distinct()
    optDf.write.format("csv").mode(SaveMode.Overwrite).saveAsTable(tableName + "_text")
    optDf.show(5)
  }

  def distributeTest(spec: String, distributespec: String, tableName: String) {
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    // Set qtr_cd null to make the computation tractable while running in local
    val hiveWriter =
      HiveDataFrameWriter("parquet", Some(spec), Some(distributespec))
    hiveWriter.insertOverwrite(tableName, dataFrame)
    sparkSession.sql(s"select * from $tableName").show(5)
  }

  def delimitedTableTest(spec: String, distributespec: String, tableName: String) {
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    val hiveWriter = HiveDataFrameWriter("text", Some(spec), Some(distributespec))
    hiveWriter.insertOverwrite(tableName, dataFrame)
    sparkSession.sql(s"show create table $tableName").show(500, truncate = false)
    //    sparkSession.table(tableName).show(5)
  }

  test("partition insert test") {
    PartitionTest(mix, "testgreenraw.target_mix")
    PartitionTest(dynamic, "testgreenraw.target_dynamic")
    PartitionTest(static, "testgreenraw.target_static")
  }

  // overWrite tests
  test("partition overwrite test") {
    PartitionTest(mix, "testgreenraw.target_mix", true)
    PartitionTest(static, "testgreenraw.target_static", true)
    PartitionTest(dynamic, "testgreenraw.target_dynamic", true)
  }

  test("partition InverseOrder Test") {
    val tableName: String = "testgreenraw.target_mix_inverse"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    dataFrame.select("yr_cd", "qtr_cd").distinct().show(100, truncate = false)
    val hiveWriter = HiveDataFrameWriter("parquet", Some("qtr_cd,yr_cd"))
    hiveWriter.insertSafely(tableName, dataFrame, isOverWrite = false)
    val hiveWriterNew = HiveDataFrameWriter("parquet", Some("yr_cd=2017,qtr_cd"))
    hiveWriterNew.insertSafely(tableName, dataFrame, isOverWrite = true)
    sparkSession.sql(s"show partitions $tableName").show(100, truncate = false)
  }

  test("testHiveUnpartitionedTableOverWrite") {
    val tableName = "testgreenraw.hive_un_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    val dfSchema   = dataFrame.schema
    val hiveWriter = HiveDataFrameWriter("parquet", None)
    val fields     = hiveWriter.getColumnDefinitions(dataFrame.schema)
    val hiveDDL    = hiveWriter.getCreateHiveTableDDL(tableName, fields, None)
    println(hiveDDL)
    sparkSession.sql(hiveDDL)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val tableCount = sparkSession.table(tableName).count()
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    assert(tableCount == newTableCount)
  }

  test("testHivePartitionedTableOverWrite") {
    val tableName = "testgreenraw.hive_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    val dfSchema   = dataFrame.schema
    val hiveWriter = HiveDataFrameWriter("parquet", Some("yr_cd"))
    val fields     = hiveWriter.getColumnDefinitions(dataFrame.schema)
    val hiveDDL    = hiveWriter.getCreateHiveTableDDL(tableName, fields, None)
    println(hiveDDL)
    sparkSession.sql(hiveDDL)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val tableCount = sparkSession.table(tableName).count()
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    assert(tableCount == newTableCount)
  }

  test("testHivePartitionedTablePartitalOverWrite") {
    val tableName = "testgreenraw.hive_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    val dfSchema   = dataFrame.schema
    val hiveWriter = HiveDataFrameWriter("parquet", Some("yr_cd"))
    val fields     = hiveWriter.getColumnDefinitions(dataFrame.schema)
    val hiveDDL    = hiveWriter.getCreateHiveTableDDL(tableName, fields, None)
    println(hiveDDL)
    sparkSession.sql(hiveDDL)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val tableCount        = sparkSession.table(tableName).count()
    val filteredDataFrame = dataFrame.where("yr_cd=2017")
    hiveWriter.insertOverwrite(tableName, filteredDataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    assert(tableCount == newTableCount)
  }

  test("testSparkUnpartitionedTableOverWrite") {
    val tableName = "testgreenraw.spark_un_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    val dfSchema   = dataFrame.schema
    val hiveWriter = HiveDataFrameWriter("parquet", None)
    val fields     = hiveWriter.getColumnDefinitions(dataFrame.schema)
    val hiveDDL    = hiveWriter.getCreateSparkTableDDL(tableName, fields, None)
    println(hiveDDL)
    sparkSession.sql(hiveDDL)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val tableCount = sparkSession.table(tableName).count()
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    assert(tableCount == newTableCount)
  }

  test("testSparkPartitionedTableOverWrite") {
    val tableName = "testgreenraw.spark_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    val dfSchema   = dataFrame.schema
    val hiveWriter = HiveDataFrameWriter("parquet", Some("yr_cd"))
    val fields     = hiveWriter.getColumnDefinitions(dataFrame.schema)
    val hiveDDL    = hiveWriter.getCreateSparkTableDDL(tableName, fields, None)
    println(hiveDDL)
    sparkSession.sql(hiveDDL)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val tableCount = sparkSession.table(tableName).count()
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    assert(tableCount == newTableCount)
  }

  test("testSparkPartitionedTablePartialOverWrite") {
    val tableName = "testgreenraw.spark_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    val dfSchema   = dataFrame.schema
    val hiveWriter = HiveDataFrameWriter("parquet", Some("yr_cd,yr_seq_nbr"))
    val fields     = hiveWriter.getColumnDefinitions(dataFrame.schema)
    val hiveDDL    = hiveWriter.getCreateSparkTableDDL(tableName, fields, None)
    println(hiveDDL)
    sparkSession.sql(hiveDDL)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    val tableCount        = sparkSession.table(tableName).count()
    val filteredDataFrame = dataFrame.where("yr_cd=2017 or yr_cd=2016")
    hiveWriter.insertOverwrite(tableName, filteredDataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    assert(tableCount == newTableCount)
  }

  test("testExternalTable") {
    val sparkTableName = "testgreenraw.spark_partitioned"
    val sparkTableMetaData = sparkSession.sessionState.catalog.getTableMetadata(
      sparkSession.sessionState.sqlParser.parseTableIdentifier(sparkTableName))
    val sparkTableLocation = sparkTableMetaData.storage.locationUri
    println(s"table Location: ${sparkTableLocation.get.getPath}")
    val hiveTableName = "testgreenraw.spark_partitioned"
    val hiveTableMetaData = sparkSession.sessionState.catalog.getTableMetadata(
      sparkSession.sessionState.sqlParser.parseTableIdentifier(hiveTableName))
    val hiveTableLocation = hiveTableMetaData.storage.locationUri
    println(s"table Location: ${hiveTableLocation.get.getPath}")
  }

  test("newTableTest") {
    val tableName = s"testgreenraw.mytest_ver_table"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName").show
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1").show
    val dataFrame  = sparkSession.table("testgreenraw.spark_un_partitioned")
    val hiveWriter = HiveDataFrameWriter("parquet", None)
    val writeLog   = hiveWriter.insertOverwrite(tableName, dataFrame)
    println(writeLog)
  }

  test("versionedExistingTableTest") {
    // explicity using upper case to test case insensitivity
    val tableName = s"testgreenraw.mytest_ver_table".toUpperCase()
    // modify the schema
    val dataFrame  = sparkSession.table(tableName).withColumn("testCol1", lit(10))
    val hiveWriter = HiveDataFrameWriter("parquet", None)
    val writeLog   = hiveWriter.insertOverwrite(tableName, dataFrame)
    println(writeLog)
  }

  test("unversionedExistingTableTest") {
    val tableName = s"testgreenraw.spark_old_table_no_ver"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName").show
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1").show
    sparkSession.table("testgreenraw.spark_un_partitioned").write.saveAsTable(tableName)
    // modify the schema
    val dataFrame = sparkSession
      .table("testgreenraw.spark_un_partitioned")
      .withColumn("testCol1", lit(10))
    val hiveWriter = HiveDataFrameWriter("parquet", None)
    val writeLog   = hiveWriter.insertOverwrite(tableName, dataFrame)
    println(writeLog)
  }

  test("unversionedExistingTableChangeTest") {
    val tableName = s"testgreenraw.spark_old_table_no_ver_mod"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName").show
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1").show
    sparkSession.table("testgreenraw.spark_un_partitioned").write.saveAsTable(tableName)
    // modify the schema
    val dataFrame = sparkSession
      .table("testgreenraw.spark_un_partitioned")
      .withColumn("testCol1", lit(10))
    val hiveWriter = HiveDataFrameWriter("parquet", None)
    val writeLog   = hiveWriter.insertOverwrite(tableName, dataFrame)
    println(writeLog)
  }

  test("unversionedExistingDelimitedTableChangeTest") {
    val tableName = s"testgreenraw.spark_old_text_table_no_ver_mod"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    sparkSession.read
      .parquet(getPathByResourceName("data/rosie_client_insights_input"))
      .write
      .format("csv")
      .option("delimiter", "\t")
      .saveAsTable(tableName)
    sparkSession.sql(s"show create table $tableName").show(500, truncate = false)
    // modify the schema
    val dataFrame = sparkSession
      .table(tableName)
      .withColumn("testCol1", lit(10))
    val hiveWriter = HiveDataFrameWriter("text", None)
    hiveWriter.insertOverwrite(tableName, dataFrame)
    sparkSession.sql(s"show create table $tableName").show(500, truncate = false)
    sparkSession.table(tableName).show(5)
  }
}
