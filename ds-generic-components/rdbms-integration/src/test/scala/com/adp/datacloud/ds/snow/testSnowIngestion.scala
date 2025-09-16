package com.adp.datacloud.ds.snow

import com.adp.datacloud.cli.SnowDataProcessorOptions
import org.apache.spark.sql.SparkSession

object testSnowIngestion {

  def testCli(cliArgs: Array[String]) {

    val config = SnowDataProcessorOptions.parse(cliArgs)
    println(config)
    println(config.sql(config.preFile))
    println(config.sql(config.sqlFile))

  }

  def createData()(implicit spark: SparkSession) {
    import spark.implicits._
    //creates temp table for tests
    val simpleData = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("Raman", "Finance", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000))
      .toDF("name", "department", "salary")
      .select("name", "salary", "department")
      .registerTempTable("demo_table") // creating demo_table

    Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("Raman", "Finance", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000))
      .toDF("name", "department", "salary")
      .select("name", "salary", "department")
      .registerTempTable("demo_table2")

    Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("Raman", "Finance", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000))
      .toDF("name", "department", "salary")
      .select("name", "salary", "department")
      .registerTempTable("demo_table3")

    Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("Raman", "Finance", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000))
      .toDF("name", "department", "salary")
      .select("name", "salary", "department")
      .registerTempTable("demo_table4")

    Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("Raman", "Finance", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000))
      .toDF("name", "department", "salary")
      .select("name", "salary", "department")
      .registerTempTable("demo_table5")

    println("Demo table created")
  }

  def testIngest(cliArgs: Array[String]) {

    val SnowDataProcessConfig = SnowDataProcessorOptions.parse(cliArgs)

    println(SnowDataProcessConfig)
    val osName = System
      .getProperty("os.name")
      .toLowerCase()
    println("Url is :::" + SnowDataProcessConfig.url)
    println("Appname is :::" + SnowDataProcessConfig.appName)

    implicit val sparkSession: SparkSession =
      if (osName.contains("windows") || osName.startsWith("mac")) {
        // log events to view from local history server
        val eventLogDir = s"/tmp/spark-events/"
        scala.reflect.io
          .Path(eventLogDir)
          .createDirectory(force = true, failIfExists = false)

        // store hive tables in this location in local mode
        val warehouseLocation = s"/tmp/spark-warehouse/"
        scala.reflect.io
          .Path(warehouseLocation)
          .createDirectory(force = true, failIfExists = false)

        SparkSession
          .builder()
          .appName(SnowDataProcessConfig.appName)
          .master("local[*]")
          .config("spark.eventLog.enabled", value = true)
          .config("spark.eventLog.dir", eventLogDir)
          .enableHiveSupport()
          .getOrCreate()

      } else {
        SparkSession
          .builder()
          .appName(SnowDataProcessConfig.appName)
          .enableHiveSupport()
          .getOrCreate()
      }

    createData
    println("SNowflake ingest test:")
    new SnowIngest(SnowDataProcessConfig).ingest
    println("Done test")

  }
  def main(args: Array[String]) {

    //testCli("--target-table t_bnchmrk_annl_data --url adpdc_cdl.us-east-1.privatelink.snowflakecomputing.com --user AKUSHWAHA --password Aug@2020 --warehouse ANALYTICS_DEV_WH --role ANALYTICS_DEV_ROLE --database ANALYTICS_DEV --schema _TARGET --hiveconf __GREEN_BASE_DB__=datacloud_nonprod_dit_base".split(" "))

    testIngest(
      "--target-env DIT --sqlfile src/test/resources/selection.sql --pre-sql src/test/resources/pre.sql --target-table demo_tbl,demo_tbl2,demo_tbl3,demo_tbl4,demo_tbl5,demo_tbl6 --account adpdc_cdl.us-east-1.privatelink --user AKUSHWAHA --warehouse ANALYTICS_DEV_WH --role ANALYTICS_DEV_ROLE --database ANALYTICS_DEV --schema _TARGET --hiveconf __GREEN_BASE_DB__=datacloud_nonprod_dit_base --sqlconf __SCHEMA__=_TARGET"
        .split(" "))
  }
}
