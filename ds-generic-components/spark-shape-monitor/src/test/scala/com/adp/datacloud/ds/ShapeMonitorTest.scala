package com.adp.datacloud.ds

import com.adp.datacloud.cli.ShapeMonitorConfig
import com.adp.datacloud.ds.ShapeMonitor.runMonitor
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShapeMonitorTest extends SparkTestWrapper with Matchers {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenbase", true)
    createTableFromResource(
      "testgreenbase",
      s"employee_monthly_shapetest_dataset",
      s"employee_monthly_shapetest_dataset")
    createTableFromResource(
      "testgreenbase",
      s"sbs_jc_result_monthly",
      s"sbs_jc_result_monthly")

  }

  override def appName: String = getClass.getCanonicalName

  private def shapeMonitorArgs(xmlName: String, partition: String): Array[String] = {
    val xmlPath = getPathByResourceName(xmlName)
    val shapeArgs: Array[String] =
      s"""--xml-config-file $xmlPath
          --db-jdbc-url $dxrJdbcURL 
          --oracle-wallet-location $oracleWalletPath
          --rerun true 
          --quick-run true 
          --partition-value $partition
          """.stripMargin.split("\\s+")
    shapeArgs
  }

  lazy val shapeMonitorConfig: ShapeMonitorConfig =
    com.adp.datacloud.cli.ShapeMonitorOptions
      .parse(shapeMonitorArgs("ev4_shape_monitor_config.xml", "201901"))

  lazy val shapeMonitorConfigSbs: ShapeMonitorConfig =
    com.adp.datacloud.cli.ShapeMonitorOptions
      .parse(shapeMonitorArgs("sbs_jc_result_monthly_shape.xml", "202101"))

  test("test stats collection") {
    import sparkSession.sqlContext.implicits._

    sparkSession
      .table("testgreenbase.employee_monthly_shapetest_dataset")
      .select(
        "employee_guid",
        "source_pr",
        "source_hr",
        "full_time_part_time_",
        "annual_base_",
        "rate_type_",
        "work_state_",
        "work_zip_",
        "status_",
        "yyyymm")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("testgreenbase.recruitment_core")

    // Process the XML configuration file and validate arguments
    val xmlProcessor = XmlInputProcessor(shapeMonitorConfig.xmlContent)

    // Get shape details from XML file
    val shapeConfigurations = xmlProcessor.getShapeDetailsFromXml()

    shapeConfigurations.toDF.show(50, false)

    //Global list of time partitions for reference
    val statsCollector = new ShapeCollector
    val shapeValidator = new ShapeValidator

    //val statsCollectorLogs: Seq[StatsLog] = shapeConfigurations.map(shapeConfiguration => statsCollector.statsComputationDriver(shapeConfiguration, ShapeMonitorConfig,true))
    val statsCollectorLogs
        : Seq[ExecutionLog] = shapeConfigurations.map(shapeConfiguration =>
      statsCollector.statsComputationDriver(shapeConfiguration, shapeMonitorConfig, true))
    val validationCollectorOutput: Seq[(ExecutionLog, Option[String])] =
      shapeConfigurations
        .map(
          shapeConfiguration =>
            shapeValidator
              .shapeValidationDriver(shapeConfiguration, shapeMonitorConfig, true))
        .toSeq

    val validationCollectorLogs = validationCollectorOutput.map(_._1)
    val failedStats             = statsCollectorLogs.filter(_.is_error == true).map(_.tableName)
    val failedVals              = validationCollectorLogs.filter(_.is_error == true).map(_.tableName)

    (failedStats.isEmpty, failedVals.isEmpty) match {
      case (true, true) => true
      case (true, false) => {
        statsCollectorLogs.foreach(println)
        Thread.sleep(10000)
        false
      }
      case (false, true) => {
        statsCollectorLogs.foreach(println)
        false
      }
      case (false, false) => {
        statsCollectorLogs.foreach(println)
        false
      }
    }

  }

  test("compact data") {
    runMonitor(shapeMonitorConfigSbs)
  }
}
