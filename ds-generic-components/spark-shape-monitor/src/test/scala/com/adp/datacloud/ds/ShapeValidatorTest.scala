package com.adp.datacloud.ds

import com.adp.datacloud.cli.{ShapeMonitorConfig, ShapeMonitorLogger}
import com.adp.datacloud.ds.ShapeMonitor.checkTable
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.log4j.Logger
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShapeValidatorTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  private val logger = Logger.getLogger(getClass)

  private def shapeMonitorArgs(): Array[String] = {
    val xmlPath = getPathByResourceName("ev4_shape_monitor_config.xml")
    val shapeArgs: Array[String] =
      s"""--xml-config-file $xmlPath
          --db-jdbc-url $dxrJdbcURL 
          --oracle-wallet-location $oracleWalletPath
          --rerun true 
          --quick-run true 
          --partition-value 202001
          """.stripMargin.split("\\s+")
    shapeArgs
  }

  lazy val shapeMonitorConfig: ShapeMonitorConfig =
    com.adp.datacloud.cli.ShapeMonitorOptions.parse(shapeMonitorArgs())

  test("validate shape") {

    sparkSession.sparkContext.setLogLevel("ERROR")
    println(sparkSession.sparkContext.getConf.toDebugString)

    val xmlProcessor        = XmlInputProcessor(shapeMonitorConfig.xmlContent)
    val shapeConfigurations = xmlProcessor.getShapeDetailsFromXml()

    val shapeMonitorLogger =
      new ShapeMonitorLogger(shapeMonitorConfig, shapeConfigurations)

    checkTable(
      shapeMonitorConfig.targetTable,
      shapeMonitorConfig.jdbcUrl,
      shapeMonitorConfig.jdbcProperties,
      "create_stats_table.sql",
      true)
    checkTable(
      shapeMonitorConfig.targetValidationTable,
      shapeMonitorConfig.jdbcUrl,
      shapeMonitorConfig.jdbcProperties,
      "create_shape_validation_status_table.sql",
      true)
    checkTable(
      shapeMonitorLogger.shapeMonitorLogsTable,
      shapeMonitorConfig.jdbcUrl,
      shapeMonitorConfig.jdbcProperties,
      "create_shape_monitor_job_log_table.sql")

    val sampleShapeConfiguration =
      shapeConfigurations //.filter(_.tableName == "optout_2019q1")(0)
    println(sampleShapeConfiguration)
    println(shapeMonitorConfig.yyyymm)
    println(shapeMonitorConfig.qtr)

    val shapeValidator = new ShapeValidator
    val validatorLogs = shapeValidator.shapeValidationDriver(
      sampleShapeConfiguration(0),
      shapeMonitorConfig)
    println(validatorLogs._1)

  }
}
