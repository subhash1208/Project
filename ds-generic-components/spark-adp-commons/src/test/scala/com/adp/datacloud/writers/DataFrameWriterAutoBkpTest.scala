package com.adp.datacloud.writers

import com.adp.datacloud.ds.hive.hiveTableUtils
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataFrameWriterAutoBkpTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  private val logger = Logger.getLogger(getClass())

  override def beforeAll(): Unit = {
    super.beforeAll()
    cleanAndSetupDatabase("testgreenraw")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenraw")
  }

  ignore("clean up old copies") {
    val tableName   = "testgreenraw.spark_history"
    val tableNameV1 = s"${tableName}_v1"
    val tableNameV2 = s"${tableName}_v2"
    val tableNameV3 = s"${tableName}_v3"
    val tableNameV4 = s"${tableName}_v4"
    val tableNameV5 = s"${tableName}_v5"

    val tableLocation = hiveTableUtils.getTableLocation(tableName)

  }

  /**
   * TODO: verify if its worth to to fix partition column order validation
   * may be moving to delta table and handling it there would be the best approach
   */

  test("verify auto backup on partition columns change") {
    val tableName   = "testgreenraw.spark_partition_swap"
    val tableNameV1 = s"${tableName}_v1"
    val tableNameV2 = s"${tableName}_v2"
    val tableNameV3 = s"${tableName}_v3"
    val tableNameV4 = s"${tableName}_v4"
    val tableNameV5 = s"${tableName}_v5"

    List(tableName, tableNameV1, tableNameV2, tableNameV3, tableNameV4, tableNameV5)
      .foreach(t => sparkSession.sql(s"DROP TABLE IF EXISTS $t"))

    val dataPath = getPathByResourceName("data/rosie_client_insights_input")
    val df       = sparkSession.read.parquet(dataPath)
    HiveDataFrameWriter("parquet", Some("yr_cd,yr_seq_nbr,aa=1"))
      .insertSafely(tableName, df.where("yr_cd not in ('2015') "), true)

    HiveDataFrameWriter("parquet", Some("yr_seq_nbr,yr_cd,aa=1"))
      .insertSafely(tableName, df, true)
    val v2Exists = sparkSession.catalog.tableExists(tableNameV2)
    displayAllTables("testgreenraw")
    assert(v2Exists)
  }

  test("verify auto backup on schema change") {
    val tableName   = "testgreenraw.spark_partitioned"
    val tableNameV1 = s"${tableName}_v1"
    val tableNameV2 = s"${tableName}_v2"
    val tableNameV3 = s"${tableName}_v3"
    val tableNameV4 = s"${tableName}_v4"
    val tableNameV5 = s"${tableName}_v5"
    List(tableName, tableNameV1, tableNameV2, tableNameV3, tableNameV4, tableNameV5)
      .foreach(t => sparkSession.sql(s"DROP TABLE IF EXISTS $t"))
    val dataPath = getPathByResourceName("data/rosie_client_insights_input")
    val df = sparkSession.read
      .parquet(dataPath)
      .withColumn("yr_cd", col("yr_cd").cast(IntegerType))
    val hiveWriter = HiveDataFrameWriter("parquet", Some("yr_cd,yr_seq_nbr"))
    hiveWriter.insertSafely(tableName, df, true)
    sparkSession.table(tableNameV1).printSchema()
    val df2 = df.withColumn("yr_cd", lit(2020)).withColumn("dummycol1", lit("dummy"))
    hiveWriter.insertSafely(tableName, df2, true)
    // only two versions of a table are maintained, fetch partition info before the table is dropped
    val partitionsv1 = hiveTableUtils.getTablePartitionNames(tableNameV1).toSet
    val partitionsv2 = hiveTableUtils.getTablePartitionNames(tableNameV2).toSet
    assert(partitionsv1.subsetOf(partitionsv2))
    val df3 = df.withColumn("yr_cd", lit(2021)).withColumn("dummycol2", lit("dummy"))
    hiveWriter.insertSafely(tableName, df3, true)
    val partitionsv3 = hiveTableUtils.getTablePartitionNames(tableNameV3).toSet
    assert(partitionsv2.subsetOf(partitionsv3))
    val df4 = df.withColumn("yr_cd", lit(2022)).withColumn("dummycol3", lit("dummy"))
    hiveWriter.insertSafely(tableName, df4, true)
    val partitionsv4 = hiveTableUtils.getTablePartitionNames(tableNameV4).toSet
    assert(partitionsv3.subsetOf(partitionsv4))
    val df5 = df.withColumn("yr_cd", lit(2023)).withColumn("dummycol4", lit("dummy"))
    hiveWriter.insertSafely(tableName, df5, true)

    // assert that the new tables contains old partitions data
    val partitionsv5 = hiveTableUtils.getTablePartitionNames(tableNameV5).toSet
    assert(partitionsv4.subsetOf(partitionsv5))
    val partitions = hiveTableUtils.getTablePartitionNames(tableName).toSet
    // assert table contains the latest versions partitions
    partitions.shouldEqual(partitionsv5)
  }

  test("verify old versions cleanup") {
    val tableName   = "testgreenraw.spark_versioned"
    val tableNameV1 = s"${tableName}_v1"
    val tableNameV2 = s"${tableName}_v2"
    val tableNameV3 = s"${tableName}_v3"
    val tableNameV4 = s"${tableName}_v4"
    val tableNameV5 = s"${tableName}_v5"
    val versionedTableNames =
      List(tableNameV1, tableNameV2, tableNameV3, tableNameV4, tableNameV5).sorted(
        Ordering.String.reverse)
    val (latestTables, oldTables) = versionedTableNames.splitAt(2)
    List(tableName, tableNameV1, tableNameV2, tableNameV3, tableNameV4, tableNameV5)
      .foreach(t => sparkSession.sql(s"DROP TABLE IF EXISTS $t"))
    val dataPath   = getPathByResourceName("data/rosie_client_insights_input")
    val df         = sparkSession.read.parquet(dataPath).limit(100)
    val hiveWriter = HiveDataFrameWriter("parquet", Some("yr_cd,yr_seq_nbr"))
    hiveWriter.insertSafely(tableName, df, true)
    val df2 = df.withColumn("yr_cd", lit(2020)).withColumn("dummycol1", lit("dummy"))
    hiveWriter.insertSafely(tableName, df2, true)
    val df3 = df.withColumn("yr_cd", lit(2021)).withColumn("dummycol2", lit("dummy"))
    hiveWriter.insertSafely(tableName, df3, true)
    val df4 = df.withColumn("yr_cd", lit(2022)).withColumn("dummycol3", lit("dummy"))
    hiveWriter.insertSafely(tableName, df4, true)
    val df5 = df.withColumn("yr_cd", lit(2023)).withColumn("dummycol4", lit("dummy"))
    hiveWriter.insertSafely(tableName, df5, true)
    latestTables.foreach(x => {
      assert(sparkSession.catalog.tableExists(x), s"Table $x should exist")
    })
    oldTables.foreach(x => {
      assert(!sparkSession.catalog.tableExists(x), s"Table $x should not exist")
    })
  }

}
