package com.adp.datacloud.util

import com.adp.datacloud.cli.{DropTableUtilConfig, DropTableUtilOptions}
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DropTableUtilTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  private val logger = Logger.getLogger(getClass())

  override def beforeAll(): Unit = {
    super.beforeAll()
    cleanAndSetupDatabase("testgreenraw")
    cleanAndSetupDatabase("testgreenbase")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenraw")
    cleanAndSetupDatabase("testgreenbase")
  }

  lazy val config: DropTableUtilConfig = {
    val patcherArgs =
      s"""--database-names testgreenraw,testgreenbase
         |--table-names spark_drop_test""".stripMargin.split("\\s+")

    DropTableUtilOptions.parse(patcherArgs)
  }

  test("drop all table versions") {

    val versions = (1 to 2).toList

    // test data prep
    config.databases.par.foreach(dbName => {
      config.tableNames.foreach(tName => {
        val tableName = s"${dbName}.${tName}"
        versions.foreach(version => {
          sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}_v${version} ")
        })
        val dataPath = getPathByResourceName("data/rosie_client_insights_input")
        val df       = sparkSession.read.parquet(dataPath)
        versions.foldLeft(df) { (dataframe, v) =>
          {
            val modifiedDf = dataframe.withColumn(s"dummy$v", lit(v))
            logger.info(s"modified table schema is \n ${modifiedDf.schema.treeString}")
            HiveDataFrameWriter("parquet", Some("yr_cd,yr_seq_nbr"))
              .insertSafely(tableName, modifiedDf, true)
            modifiedDf
          }
        }
        val maxVersionedTableName = s"${tableName}_v${versions.max}"
        assert(
          sparkSession.catalog.tableExists(maxVersionedTableName),
          s"Table $maxVersionedTableName should exist")
      })
    })

    DropTableUtil.dropTables(config)

    // validate table drops
    config.databases.par.foreach(dbName => {
      config.tableNames.foreach(tName => {
        val tableName = s"${dbName}.${tName}"
        versions.foreach(version => {
          val versionedTableName = s"${tableName}_v${version}"
          assert(
            !sparkSession.catalog.tableExists(versionedTableName),
            s"Table $versionedTableName should not exist")
        })
      })
    })

  }

}
