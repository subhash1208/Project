package com.adp.datacloud.cli

import com.adp.datacloud.ds.ShapeConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import scala.collection.JavaConverters.propertiesAsScalaMapConverter

case class ShapeMonitorJobLog(
    APPLICATION_ID: String,
    ATTEMPT_ID: Option[String],
    APPLICATION_NAME: String,
    XML_INPUT_CONFIG: String,
    JOB_STATUS: String,
    JOB_STATUS_CODE: Integer,
    FAILED_TABLES: String = null,
    MSG: String           = null,
    IS_ERROR: Integer     = 0)

class ShapeMonitorLogger(
    ShapeMonitorConfig: ShapeMonitorConfig,
    shapeConfigurations: List[ShapeConfiguration])(implicit sparkSession: SparkSession) {

  private val logger                        = Logger.getLogger(getClass)
  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  import sparkSession.implicits._
  val sc = sparkSession.sparkContext

  val shapeMonitorLogsTable = s"spark_shape_monitor_job_logs"
  val xmlInputConfig        = Serialization.write(shapeConfigurations)

  def insertJobLog(
      status: String,
      statusCode: Integer,
      failed_tables: String = null,
      message: String       = null,
      isError: Integer      = 0) = {
    val insertDf = List(
      ShapeMonitorJobLog(
        sc.applicationId,
        sc.applicationAttemptId,
        ShapeMonitorConfig.applicationName,
        xmlInputConfig,
        status,
        statusCode,
        failed_tables,
        message,
        isError)).toDF()

    try {
      val logTableOptions = new JdbcOptionsInWrite(
        ShapeMonitorConfig.jdbcUrl,
        shapeMonitorLogsTable,
        ShapeMonitorConfig.jdbcProperties.asScala.toMap)
      JdbcUtils.saveTable(insertDf, None, true, logTableOptions)
      true
    } catch {
      case e: Exception => {
        insertDf.show()
        logger.error("Error while writing shape monitor job log table: ", e)
        e.printStackTrace()
        false
      }
    }

  }

}
