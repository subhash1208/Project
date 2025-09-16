package com.adp.datacloud.ds.util

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * static fields separated out to make it is easy to access from with executor tasks
 */
object DXRLoggerHelper {

  val extractLiveLogsTable        = s"spark_extractor_job_live_logs"
  val extractStatusTable          = s"spark_extractor_job_status"
  val extractLogsTable            = s"spark_extractor_job_logs"
  val exportLogsTable             = s"spark_exporter_job_logs"
  val hiveWriterLogsTable         = s"spark_hive_writer_job_logs"
  val extractIncrementBoundsTable = s"spark_extractor_increment_bounds"
  val dxrExtractSummaryTable      = s"dxr_extractor_sql_summary"

  val dxrExtractSummaryESIndexName = s"dxr-extractor-summary-logs"
  val exportLogsESIndexName        = s"dxr-exporter-job-logs"
  val hiveWriterLogsIndexName      = s"hive-writer-job-logs"

  // getOracleDialect
  private val oracleDialect: JdbcDialect = JdbcDialects.get("jdbc:oracle")

  def getDXRLoggerConnection(
      dxrUrl: String,
      logTable: String,
      dxrConnectionProps: Properties): Connection = {
    val tableOptions =
      new JdbcOptionsInWrite(dxrUrl, logTable, dxrConnectionProps.asScala.toMap)
    JdbcUtils.createConnectionFactory(tableOptions)()
  }

  val DXRTaskLogSchema: StructType =
    ScalaReflection.schemaFor[DXRTaskLog].dataType.asInstanceOf[StructType]

  private val DXRTaskLogSchemaNullTypes = getJdbcNullTypes(DXRTaskLogSchema.fields)

  val insertDXRTaskLiveLogStmt: String =
    JdbcUtils.getInsertStatement(
      extractLiveLogsTable,
      DXRTaskLogSchema,
      None,
      isCaseSensitive = false,
      oracleDialect)

  def getDXRTaskLiveLogPreparedStmt(
      connection: Connection,
      taskLog: DXRTaskLog): PreparedStatement = {
    val preparedStmt: PreparedStatement =
      connection.prepareStatement(insertDXRTaskLiveLogStmt)
    preparedStmt.setQueryTimeout(5) // logger shouldn't take more than 5 seconds
    valueSetter(preparedStmt, taskLog.productIterator, DXRTaskLogSchemaNullTypes)
    preparedStmt
  }

  private def getJdbcNullTypes(fields: Array[StructField]): Array[Int] = {
    fields.map(f => {
      val dt: DataType = f.dataType
      oracleDialect
        .getJDBCType(dt)
        .orElse(getCommonJDBCType(dt))
        .getOrElse(throw new IllegalArgumentException(
          s"Can't get JDBC type for ${dt.catalogString}"))
        .jdbcNullType
    })
  }

  /**
   * creates setters for most of the types in our logger
   *
   * @param preparedStatement
   * @param iterator
   */
  private def valueSetter(
      preparedStatement: PreparedStatement,
      iterator: Iterator[Any],
      nullTypes: Array[Int]): Unit = {

    var pos = 1

    for (elem <- iterator) {
      if (elem == null) {
        preparedStatement.setNull(pos, nullTypes(pos - 1))
      } else {
        elem match {
          case x: Integer =>
            preparedStatement.setInt(pos, x)
          case x: Long =>
            preparedStatement.setLong(pos, x)
          case x: String =>
            preparedStatement.setString(pos, x)
          case x: Boolean =>
            preparedStatement.setBoolean(pos, x)
          case x: Option[String] =>
            if (x.isDefined) {
              preparedStatement.setString(pos, x.get)
            } else {
              preparedStatement.setString(pos, "")
            }
          case _ =>
            throw new IllegalArgumentException(
              s"Can't find setter for value of unknown type at $pos")
        }

      }
      pos += 1
    }
  }

}

case class DXR2Exception(s: String) extends Exception(s)

// TODO: include state machine details and try update rather than duplicate insert
case class DXRJobLog(
    APPLICATION_ID: String,
    ATTEMPT_ID: Option[String],
    APPLICATION_NAME: String,
    PRODUCT_NAME: String,
    DXR_SQL: String,
    INPUT_SQL: String,
    JOB_CONFIG: String,
    JOB_STATUS: String,
    JOB_STATUS_CODE: Integer,
    CLUSTER_ID: String,
    WORKFLOW_NAME: String,
    STATE_MACHINE_EXECUTION_ID: String,
    ENV: String,
    JOB_START_TIME: Timestamp,
    JOB_END_TIME: Timestamp,
    MSG: String                   = null,
    PARENT_APPLICATION_ID: String = null,
    IS_ERROR: Boolean             = false)

case class DXRTaskLog(
    APPLICATION_ID: String,
    ATTEMPT_ID: Option[String],
    SQL_NAME: String,
    TARGET_KEY: Integer,
    DB_PARTITION: String,
    FINAL_SELECT_SQL: String,
    FINAL_SESSION_SETUP_CALL: String,
    MSG: String,
    TASK_ATTEMPT_NUMBER: Integer = 0,
    TASK_ATTEMPT_ID: Long        = 0,
    EXTRACT_STATUS_CODE: Integer = 2,
    EXTRACT_STATUS: String       = "FINISHED",
    BATCH_NUMBER: Long           = 0,
    EXTRACT_ATTEMPT_ID: Integer  = 1,
    EXEC_TIME_MS: Integer        = 0,
    ROW_COUNT: Integer           = 0,
    IS_ERROR: Boolean            = false)

case class DXRIncrementStatus(
    APPLICATION_ID: String,
    ENV_NAME: String,
    SQL_NAME: String,
    INCREMENTAL_START_DATE: Timestamp,
    INCREMENTAL_END_DATE: Timestamp)
