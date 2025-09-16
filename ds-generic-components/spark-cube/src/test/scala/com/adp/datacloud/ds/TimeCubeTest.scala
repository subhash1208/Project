package com.adp.datacloud.ds

import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types.IntegerType
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import scala.sys.process._

@RunWith(classOf[JUnitRunner])
class TimeCubeTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override def beforeAll() = {
    super.beforeAll()
    setUpDatabase("testgreenraw")
    setUpDatabase("testgreenmain")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenmain")
  }

  ignore("validate rollup data") {

    val timeDf = sparkSession.read.parquet(
      "src/main/resources/t_dim_day_rollup/t_dim_day_rollup.snappy.parquet")

    timeDf.printSchema()
    timeDf.where("yr_cd = 2022").show(false)
    timeDf.where("yr_cd <> 2022").show(false)

    timeDf
      .select("yr_cd", "qtr_cd", "mnth_cd", "wk_cd", "sys_calendar_days")
      .where("yr_cd = 2021")
      .sort("sys_calendar_days", "wk_cd")
      .coalesce(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .saveAsTable("testgreenmain.t_dim_day_rollup_new_csv")

    val timeDfOld = sparkSession.read.parquet(
      "src/main/resources/t_dim_day_rollup/t_dim_day_rollup_old.gz.parquet")

    timeDfOld.show(false)

    timeDfOld
      .select("yr_cd", "qtr_cd", "mnth_cd", "wk_cd", "sys_calendar_days")
      .where("yr_cd = 2021")
      .sort("sys_calendar_days", "wk_cd")
      .coalesce(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .saveAsTable("testgreenmain.t_dim_day_rollup_old_csv")

    timeDfOld.printSchema()
    val newCount = timeDf.where("yr_cd <> 2022").count()
    val oldCount = timeDfOld.count()
    println(s"new count:$newCount")
    println(s"old count:$oldCount")

  }

  ignore("simulate rollup data") {

    val timeDim = sparkSession.read.parquet("src/main/resources/t_dim_day")
    val timeCubeColumns =
      List("yr_cd", "qtr_cd", "mnth_cd", "wk_cd", "sys_calendar_days") map { col(_) }
    val timeCube = {
      timeDim
        .rollup("yr_cd", "qtr_cd", "mnth_cd")
        .agg(count(lit(1)).as("sys_calendar_days"))
        .filter(col("yr_cd").isNotNull)
        .withColumn("wk_cd", lit(null.asInstanceOf[String]))
        .select(timeCubeColumns: _*)
        .union({
          timeDim
            .groupBy("yr_cd", "wk_cd")
            .agg(count(lit(1)).as("sys_calendar_days"))
            .withColumn("qtr_cd", lit(null.asInstanceOf[String]))
            .withColumn("mnth_cd", lit(null.asInstanceOf[String]))
            .select(timeCubeColumns: _*)
        })
    }

    println("Size = " + timeCube.count())
    timeCube.filter("wk_cd is not null").show()
    timeCube
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("testgreenmain.t_dim_day_rollup")

    // Note: Rename the part file manually to t_dim_day_rollup.parquet in the target folder.
    println("pwd".!!)
    println(
      "rm -v -f src/main/resources/t_dim_day_rollup/t_dim_day_rollup.snappy.parquet".!!)
    println("ls build/spark-warehouse/testgreenmain.db/t_dim_day_rollup/".!!)
    val files       = "ls build/spark-warehouse/testgreenmain.db/t_dim_day_rollup/".!!
    val parquetFile = files.split("\n").filter(_.endsWith(".snappy.parquet")).head
    println(
      s"cp -v build/spark-warehouse/testgreenmain.db/t_dim_day_rollup/$parquetFile src/main/resources/t_dim_day_rollup/t_dim_day_rollup.snappy.parquet".!!)
  }

  /**
   * run TDimDayRollUpTest before this to generate the data
   */
  ignore("fix schema") {
    val timeDf = sparkSession.read.parquet(
      "../rdbms-integration/build/spark-warehouse/testgreenraw.db/t_dim_day_rollup_v1")
    timeDf.show()
    timeDf
      .withColumn("sys_calendar_days", col("sys_calendar_days").cast(IntegerType))
      .select("yr_cd", "qtr_cd", "mnth_cd", "wk_cd", "sys_calendar_days")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("testgreenmain.t_dim_day_rollup")

    sparkSession.table("testgreenmain.t_dim_day_rollup").show()

    // Note: Rename the part file manually to t_dim_day_rollup.parquet in the target folder.
    println("pwd".!!)
    println(
      "rm -v -f src/main/resources/t_dim_day_rollup/t_dim_day_rollup.snappy.parquet".!!)
    println("ls build/spark-warehouse/testgreenmain.db/t_dim_day_rollup/".!!)
    val files       = "ls build/spark-warehouse/testgreenmain.db/t_dim_day_rollup/".!!
    val parquetFile = files.split("\n").filter(_.endsWith(".snappy.parquet")).head
    println(
      s"cp -v build/spark-warehouse/testgreenmain.db/t_dim_day_rollup/$parquetFile src/main/resources/t_dim_day_rollup/t_dim_day_rollup.snappy.parquet".!!)
  }

}
