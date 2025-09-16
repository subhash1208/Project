package com.adp.datacloud.writers

import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql.functions.lit
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeltaDataFrameWriterTest extends SparkTestWrapper with Matchers {
  override def appName: String = getClass.getCanonicalName

  override def beforeAll(): Unit = {
    super.beforeAll()
    cleanAndSetupDatabase("testgreenraw")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenraw")
  }

  val mix           = "cola=AAA,colb='BBB',bool1=true,yr_cd"
  val static        = "yr_cd=2017,qtr_cd=20171,mnth_cd=201703"
  val dynamic       = "yr_cd,qtr_cd,mnth_cd"
  val dynamic_wrong = "qtr_cd,mnth_cd,yr_cd"

  def showCreateTable(tableName: String): Unit = {
    sparkSession.sql(s"DESCRIBE EXTENDED $tableName").show(100, false)
  }

  def PartitionTest(
                     spec: String,
                     tableName: String  = "testgreenraw.target",
                     overWrite: Boolean = false) {
    val partitionSpec = Some(spec)
    println(s"Test for $spec partition spec")
    if (!overWrite) {
      sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    }
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame, partitionSpec)
    sparkSession.table(tableName).show(5, false)
    val tableMetaData = sparkSession.sessionState.catalog.getTableMetadata(
      sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName))
    println(tableMetaData.properties)
  }

  def distributeTest(spec: String, distributespec: String, tableName: String) {
    val partitionSpec = Some(distributespec)
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame, partitionSpec)
    sparkSession.table(tableName).show(5)
  }

  ignore("get jar path") {
    val src = io.delta.tables.DeltaTable.getClass.getProtectionDomain.getCodeSource
    if (src != null) {
      val jar = src.getLocation
      println(jar)
    }

    val klass = io.delta.tables.DeltaTable.getClass
    val location = klass.getResource('/' + klass.getName.replace('.', '/') + ".class")
    println(location)
  }

  test("mixed partition insert test") {
    PartitionTest(mix, "testgreenraw.target_mix")
  }

  test("dynamic partition insert test") {
    PartitionTest(dynamic, "testgreenraw.target_dynamic")
  }

  test("static partition insert test") {
    PartitionTest(static, "testgreenraw.target_static")
  }

  // append tests
  test("mixed partition append test") {
    PartitionTest(mix, "testgreenraw.target_mix")
  }

  test("dynamic partition append test") {
    PartitionTest(dynamic, "testgreenraw.target_dynamic")
  }

  test("static partition append test") {
    PartitionTest(static, "testgreenraw.target_static")
  }

  // overWrite tests
  test("mixed partition overwrite test") {
    PartitionTest(mix, "testgreenraw.target_mix", true)
  }

  test("dynamic partition overwrite test") {
    PartitionTest(dynamic, "testgreenraw.target_dynamic", true)
  }

  test("static partition overwrite test") {
    PartitionTest(static, "testgreenraw.target_static", true)
  }

  test("partition InverseOrder Test") {
    // failing
    val tableName: String = "testgreenraw.target_mix_inverse"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    dataFrame.select("yr_cd", "qtr_cd").distinct().show(100, truncate = false)
    DeltaDataFrameWriter.insert(
      tableName,
      dataFrame,
      isOverWrite = false,
      Some("qtr_cd,yr_cd"))
    DeltaDataFrameWriter.insert(
      tableName,
      dataFrame,
      isOverWrite = true,
      Some("yr_cd=2017,qtr_cd"))
    sparkSession.sql(s"show partitions $tableName").show(100, truncate = false)
  }

  test("testSparkUnpartitionedTableOverWrite") {
    val tableName = "testgreenraw.spark_un_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame)
    val tableCount = sparkSession.table(tableName).count()
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    showCreateTable(tableName)
    assert(tableCount == newTableCount)
  }

  test("testPartitionedTableOverWrite") {
    val partitionSpec = Some("yr_cd")
    val tableName     = "testgreenraw.spark_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame, partitionSpec)
    val tableCount = sparkSession.table(tableName).count()
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame, partitionSpec)
    val newTableCount = sparkSession.table(tableName).count()
    showCreateTable(tableName)
    assert(tableCount == newTableCount)
  }

  test("testPartitionedTablePartialOverWrite") {
    val tableName = "testgreenraw.spark_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame, Some("yr_cd,yr_seq_nbr"))
    val tableCount = sparkSession.table(tableName).count()
    sparkSession.table(tableName).groupBy("yr_cd", "yr_seq_nbr").count().show(false)
    val filteredDataFrame = dataFrame.where("yr_cd=2017 or yr_cd=2016")
    DeltaDataFrameWriter.insertOverwrite(
      tableName,
      filteredDataFrame,
      Some("yr_cd,yr_seq_nbr"))
    val newTableCount = sparkSession.table(tableName).count()
    sparkSession.table(tableName).groupBy("yr_cd", "yr_seq_nbr").count().show(false)
    showCreateTable(tableName)
    assert(tableCount == newTableCount)
  }

  test("unpartitioned table Schema evolution in overwrite mode") {
    val tableName = "testgreenraw.spark_un_partitioned_schema_change"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    val dataFrame =
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame)
    val tableCount = sparkSession.table(tableName).count()
    showCreateTable(tableName)
    DeltaDataFrameWriter.insertOverwrite(
      tableName,
      dataFrame.withColumn("testCol1", lit(10)))
    val newTableCount = sparkSession.table(tableName).count()
    showCreateTable(tableName)
    assert(tableCount == newTableCount)
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame.drop("clnt_obj_id"))
    val finalTableCount = sparkSession.table(tableName).count()
    showCreateTable(tableName)
    assert(newTableCount == finalTableCount)
    //TODO: check columns names are a subset on drop and addition of columns
  }

  test("unpartitioned table Schema evolution in append mode") {}

  test("partitioned table Schema evolution in overwrite mode") {}

  test("partitioned table Schema evolution in append mode") {}

  test("Schema evolution in append mode when changing partition modes") {}

  test("Schema evolution in overwrite mode when changing partition modes") {}

  test("overwrite a hive dataframe writer unpartitioned table") {
    val tableName = "testgreenraw.spark_un_partitioned"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v1")
    val dataFrame = {
      sparkSession.read.parquet(getPathByResourceName("data/rosie_client_insights_input"))
    }
    HiveDataFrameWriter("parquet", None).insertOverwrite(tableName, dataFrame)
    showCreateTable(tableName)
    val tableCount = sparkSession.table(tableName).count()
    DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame)
    val newTableCount = sparkSession.table(tableName).count()
    showCreateTable(tableName)
    assert(tableCount == newTableCount)
  }

  test("overwrite a hive dataframe writer partitioned table") {}

  test("append a hive dataframe writer unpartitioned table") {}

  test("append a hive dataframe writer partitioned table") {}

  test(" write complex data types in delta table to unpartitioned table") {}

  test("overwrite a hive dataframe writer unpartitioned table with complex data types") {}

  test("overwrite a hive dataframe writer partitioned table with complex data types") {}

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
    val dataFrame = sparkSession.table("testgreenraw.spark_un_partitioned")
    val writeLog  = DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame, None)
    println(writeLog)
  }

  ignore("versionedExistingTableTest") {
    // failing
    // TODO: test failing need a fix
    // explicity using upper case to test case insensitivity
    val tableName = s"testgreenraw.mytest_ver_table".toUpperCase()
    // modify the schema
    val dataFrame = sparkSession.table(tableName).withColumn("testCol1", lit(10))
    val writeLog  = DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame)
    println(writeLog)
  }

  test("unversionedExistingTableTest") {
    val tableName = s"testgreenraw.spark_old_table_no_ver"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName").show
    sparkSession.table("testgreenraw.spark_un_partitioned").write.saveAsTable(tableName)
    // modify the schema
    val dataFrame = sparkSession
      .table("testgreenraw.spark_un_partitioned")
      .withColumn("testCol1", lit(10))
    val writeLog = DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame)
    println(writeLog)
  }

  test("unversionedExistingTableChangeTest") {
    val tableName = s"testgreenraw.spark_old_table_no_ver_mod"
    sparkSession.sql(s"DROP TABLE IF EXISTS $tableName").show
    sparkSession.table("testgreenraw.spark_un_partitioned").write.saveAsTable(tableName)
    // modify the schema
    val dataFrame = sparkSession
      .table("testgreenraw.spark_un_partitioned")
      .withColumn("testCol1", lit(10))

    val writeLog = DeltaDataFrameWriter.insertOverwrite(tableName, dataFrame)
    println(writeLog)
  }

}