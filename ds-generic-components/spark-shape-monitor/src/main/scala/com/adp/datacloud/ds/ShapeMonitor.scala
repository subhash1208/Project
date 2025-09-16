package com.adp.datacloud.ds

import scala.collection.parallel.ForkJoinTaskSupport
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import com.adp.datacloud.cli.{ShapeMonitorConfig, ShapeMonitorLogger}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.concurrent.forkjoin.ForkJoinPool

object ShapeMonitor {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val ShapeMonitorConfig = com.adp.datacloud.cli.ShapeMonitorOptions.parse(args)

    implicit val sparkSession: SparkSession =
      SparkSession
        .builder()
        .appName(ShapeMonitorConfig.applicationName)
        .enableHiveSupport()
        .getOrCreate()

    val filesString = ShapeMonitorConfig.files
      .getOrElse("")
      .trim

    if (filesString.nonEmpty) {
      filesString
        .split(",")
        .foreach(x => {
          logger.info(s"DEPENDENCY_FILE: adding $x to sparkFiles")
          sparkSession.sparkContext.addFile(x)
        })
    }

    runMonitor(ShapeMonitorConfig)

  }

  def runMonitor(ShapeMonitorConfig: ShapeMonitorConfig)(implicit
      sparkSession: SparkSession): Unit = {
    import sparkSession.sqlContext.implicits._
    // Process the XML configuration file and validate arguments
    val xmlProcessor = XmlInputProcessor(ShapeMonitorConfig.xmlContent)
    // Get shape details from XML file
    val shapeConfigurations = xmlProcessor.getShapeDetailsFromXml()

    shapeConfigurations.toDF.show(500, false)

    val shapeMonitorLogger =
      new ShapeMonitorLogger(ShapeMonitorConfig, shapeConfigurations)

    val shapeConfigurationsParalleled = shapeConfigurations.par
    shapeConfigurationsParalleled.tasksupport = new ForkJoinTaskSupport(
      new ForkJoinPool(ShapeMonitorConfig.parallelComputations))

    val shapeCollector = new ShapeCollector
    val shapeValidator = new ShapeValidator

    checkTable(
      ShapeMonitorConfig.targetTable,
      ShapeMonitorConfig.jdbcUrl,
      ShapeMonitorConfig.jdbcProperties,
      "create_stats_table.sql",
      true)
    checkTable(
      ShapeMonitorConfig.targetValidationTable,
      ShapeMonitorConfig.jdbcUrl,
      ShapeMonitorConfig.jdbcProperties,
      "create_shape_validation_status_table.sql",
      true)
    checkTable(
      shapeMonitorLogger.shapeMonitorLogsTable,
      ShapeMonitorConfig.jdbcUrl,
      ShapeMonitorConfig.jdbcProperties,
      "create_shape_monitor_job_log_table.sql")

    shapeMonitorLogger.insertJobLog("Started", 1)

    val statsCollectorLogs: Seq[ExecutionLog] =
      shapeConfigurationsParalleled
        .map(shapeConfiguration =>
          shapeCollector.statsComputationDriver(shapeConfiguration, ShapeMonitorConfig))
        .seq
    val validationCollectorOutput: Seq[(ExecutionLog, Option[String])] =
      shapeConfigurationsParalleled
        .map(shapeConfiguration =>
          shapeValidator.shapeValidationDriver(shapeConfiguration, ShapeMonitorConfig))
        .seq

    val validationCollectorLogs = validationCollectorOutput.map(_._1)
    val executionLogs           = statsCollectorLogs ++ validationCollectorLogs
    val failedShapes =
      executionLogs.filter(_.is_error == true).map(_.tableName).toSet.toList
    val failedShapesErrorMsg =
      executionLogs.filter(_.is_error == true).map(_.error_msg).toList
    val shapeValidationFailures =
      validationCollectorOutput.map(_._2).filter(_.isDefined).map(_.get).toList
    executionLogs.foreach(println)

    if (failedShapes.nonEmpty | shapeValidationFailures.nonEmpty) {
      val errSummary =
        s"Some of tables failed during stats collections and validations. SHAPE_MONITOR_RUN_FAILURES=${failedShapes
          .mkString(",")} VALIDATION_FAILURES=${shapeValidationFailures.mkString(",")} \n"
      shapeMonitorLogger.insertJobLog(
        "Failed",
        4,
        (failedShapes ++ shapeValidationFailures).toSet.mkString(","),
        errSummary + failedShapesErrorMsg.mkString("\n"),
        1)
      throw new Exception(errSummary)
    } else shapeMonitorLogger.insertJobLog("Completed", 2)
  }

  //Check if the table is already exist in the rdbms. If not, create the table.
  def checkTable(
      table: String,
      jdbcUrl: String,
      jdbcProperties: java.util.Properties,
      sqlFile: String,
      useStagingTable: Boolean = false)(implicit sparkSession: SparkSession) {
    val tableName = if (useStagingTable) table + "_STG" else table
    val tableOptions =
      new JdbcOptionsInWrite(jdbcUrl, tableName, jdbcProperties.asScala.toMap)
    val conn = JdbcUtils.createConnectionFactory(tableOptions)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, tableOptions)
      if (!tableExists) {
        val sourceFile = scala.io.Source
          .fromInputStream(getClass.getResourceAsStream(s"/${sqlFile}"))

        val createTableSql =
          try sourceFile.mkString.replace("&TABLE_NAME", table)
          finally sourceFile.close()
        val statement = conn.createStatement
        createTableSql.split(";").foreach(statement.addBatch(_))
        try {
          logger.info(s"CREATE_TABLE_SQL: $createTableSql")
          statement.executeBatch()
        } finally {
          statement.close()
        }
      }
    } finally {
      conn.close()
    }
  }

}
