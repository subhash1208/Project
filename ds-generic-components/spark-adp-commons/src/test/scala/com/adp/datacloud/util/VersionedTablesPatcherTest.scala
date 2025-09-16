package com.adp.datacloud.util

import com.adp.datacloud.cli.{VersionedTablesPatcherConfig, VersionedTablesPatcherOptions}
import com.adp.datacloud.ds.hive.hiveTableUtils
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VersionedTablesPatcherTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  private val logger = Logger.getLogger(getClass())

  override def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw", true)
//    setUpDatabase("testgreenraw")
  }

  lazy val config = {
    VersionedTablesPatcherOptions.parse(getVersionPatcherConfigArgs(1))
  }

  lazy val configFull = {
    VersionedTablesPatcherOptions.parse(getVersionPatcherConfigArgs())
  }

  def getVersionPatcherConfigArgs(depth: Int = 0) = {
    val patcherArgs =
      s"""--database-names testgreenraw
         |--table-names spark_versioned
         |--depth $depth""".stripMargin.split("\\s+")
    patcherArgs
  }

  test("validate version patching") {
    val statusCheckNew: Boolean = runPatchTest(config)

    assert(statusCheckNew, "patching was not successful")
  }

  test("validate version patching full") {
    val statusCheckNew: Boolean = runPatchTest(configFull)

    assert(statusCheckNew, "patching was not successful")
  }

  private def runPatchTest(patcherConfig: VersionedTablesPatcherConfig) = {
    val tableName = "testgreenraw.spark_versioned"

    val dataPath   = getPathByResourceName("data/rosie_client_insights_input")
    val df         = sparkSession.read.parquet(dataPath).limit(1000)
    val hiveWriter = HiveDataFrameWriter("parquet", Some("yr_cd,yr_seq_nbr"))
    hiveWriter.insertSafely(tableName, df, true)
    val df2 = df.withColumn("yr_cd", lit(2020)).withColumn("dummycol1", lit("dummy"))
    hiveWriter.insertSafely(tableName, df2, true)

    val df3 = df2.withColumn("yr_cd", lit(2021)).withColumn("dummycol2", lit("dummy"))
    hiveWriter.insertSafely(tableName, df3, true)

    val df4 = df3.withColumn("yr_cd", lit(2022)).withColumn("dummycol3", lit("dummy"))
    hiveWriter.insertSafely(tableName, df4, true)

    val df5 = df4.withColumn("yr_cd", lit(2023)).withColumn("dummycol4", lit("dummy"))
    hiveWriter.insertSafely(tableName, df5, true)

    val versionedTableNamesMap = HiveDataFrameWriter.getVersionedTableNamesMap(tableName)
    val latestTableVersion     = versionedTableNamesMap.keys.max
    val latestTableName        = versionedTableNamesMap(latestTableVersion)
    val filteredPartitions =
      hiveTableUtils.getTablePartitionNames(latestTableName).filter(_.contains("2016"))

    if (filteredPartitions.nonEmpty) {
      val dropSql =
        hiveTableUtils.getDropPartitionsSql(latestTableName, filteredPartitions)
      println(dropSql)
      sparkSession.sql(dropSql)
      sparkSession.catalog.recoverPartitions(tableName)
    }

    val statusCheck =
      HiveDataFrameWriter.checkLatestTableAgainstBackup(tableName).getOrElse(false)
    assert(!statusCheck, "validation did not detect missing partitions")

    VersionedTablesPatcher.runPatching(patcherConfig)

    val statusCheckNew =
      HiveDataFrameWriter.checkLatestTableAgainstBackup(tableName).getOrElse(false)
    statusCheckNew
  }
}
