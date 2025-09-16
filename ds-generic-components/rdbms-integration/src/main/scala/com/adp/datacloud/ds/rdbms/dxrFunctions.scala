package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.ds.util.DXRLoggerHelper.{extractLiveLogsTable, getDXRLoggerConnection, getDXRTaskLiveLogPreparedStmt}
import com.adp.datacloud.ds.util.{DXRConnection, DXRTaskLog}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JdbcUtils}
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, ResultSet, Statement}
import java.util.Properties
import java.util.regex.Pattern
import scala.collection.JavaConverters.propertiesAsScalaMapConverter

case class JDBCObject(
    connection: Connection,
    statement: Statement,
    resultSet: ResultSet) {
  private val logger = Logger.getLogger(getClass)
  // Safely close all JDBC objects
  def closeSafely(dbUserName: String, jdbcUrl: String): Unit = {
    try {
      if (null != resultSet) {
        resultSet.close()
      }
    } catch {
      case e: Exception =>
        logger.warn(
          s"DXR_CONNECTION: Exception closing resultset for $dbUserName with $jdbcUrl",
          e)
    }
    try {
      if (null != statement) {
        statement.close()
      }
    } catch {
      case e: Exception =>
        logger.warn(
          s"DXR_CONNECTION: Exception closing statement for $dbUserName with $jdbcUrl",
          e)
    }
    try {
      if (null != connection) {
        connection.close()
      }
      logger.info(
        s"DXR_CONNECTION: safely closed connection for $dbUserName with $jdbcUrl")
    } catch {
      case e: Exception =>
        logger.warn(
          s"DXR_CONNECTION: Exception closing connection for $dbUserName with $jdbcUrl",
          e)
    }
  }

}

object dxrFunctions {
  private val logger = Logger.getLogger(getClass)

  /**
   * Returns a case class containing jdbc connection Objects
   *
   * @param dxrConn
   * @param fetchSize
   * @return
   */
  def getJDBCObjectFromDXRConnection(
      dxrConn: DXRConnection,
      dxrProductName: String,
      fetchSize: Int  = 1000,
      maxRows: Int    = 0,
      maxtimeout: Int = 1800): JDBCObject = {

    val connectionProperties: Properties =
      getConnectionPropertiesFromDXRConnection(dxrConn, dxrProductName)

    val jdbcOptions = new JDBCOptions(
      dxrConn.JDBC_URL,
      dxrConn.FINAL_SQL,
      connectionProperties.asScala.toMap)

    val connection =
      JdbcUtils.createConnectionFactory(jdbcOptions)()

    connection.setTransactionIsolation(dxrConn.TRANSACTION_ISOLATION_LVL)

    // The below is used during logging in PreparedStatementSpy
    val sqlForLogging =
      if (dxrConn.BATCH_NUMBER == 0)
        "--" + dxrConn.SQL_NAME + " " + dxrConn.FINAL_SESSION_SETUP_CALL + "\n" + dxrConn.FINAL_SQL
      else
        "--" + dxrConn.SQL_NAME + " BATCH_NUMBER=" + dxrConn.BATCH_NUMBER + "\n" + dxrConn.FINAL_SQL

    dxrConn.SOURCE_DB match {
      case "sqlserver" => {
        // manually handle session setup and logging for sql driver
        if (dxrConn.FINAL_SESSION_SETUP_CALL.nonEmpty) {
          connection.prepareCall(dxrConn.FINAL_SESSION_SETUP_CALL).execute()
        }
        logger.info("final sql statement being executed: " + sqlForLogging)
      }
      case _ =>
    }

    // Session setup in case explicitly specified in SQLs
    val pattern = Pattern.compile("(?m)^\\s*\\-\\-\\s*SESSION_SETUP(.*)$")
    val matcher = pattern.matcher(dxrConn.FINAL_SQL)
    while (matcher.find()) {
      val pl = matcher.group(1)
      logger.info(s"Adding Session Setup ( $pl ) on ${dxrConn.SQL_NAME}")
      connection.prepareCall("BEGIN " + pl + " END;").execute()
    }

    val stmt = connection.prepareStatement(
      sqlForLogging,
      ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(fetchSize)
    stmt.setQueryTimeout(maxtimeout)
    stmt.setMaxRows(maxRows)

    val rs = stmt.executeQuery()

    JDBCObject(connection, stmt, rs)
  }

  def getConnectionPropertiesFromDXRConnection(
      dxrConn: DXRConnection,
      dxrProductName: String): Properties = {
    val connectionProperties = new Properties()

    val dbDriver = dxrConn.SOURCE_DB match {
      case "oracle" => {

        /*
        max lengths should be as per the oracle docs
          [[https://docs.oracle.com/cd/B19306_01/java.102/b14355/endtoend.htm#BEIIJHHA]]
         */

        val taskContext = org.apache.spark.TaskContext.get()
        val (attemptNumber, attemptId) = if (taskContext != null) {
          (taskContext.attemptNumber(), taskContext.taskAttemptId())
        } else (0, 0.toLong)

        val actionText = dxrConn.OWNER_ID + ":" + dxrProductName
        val moduleText = attemptNumber + ":" + dxrConn.SQL_NAME + ":" + attemptId

        connectionProperties.setProperty(
          "MODULE",
          moduleText.substring(0, Math.min(moduleText.length, 48)))
        connectionProperties.setProperty(
          "ACTION",
          actionText.substring(0, Math.min(actionText.length, 32)))
        "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver"
      }
      case "sqlserver" => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case "mysql"     => "com.mysql.cj.jdbc.Driver"
      case "postgres"  => "org.shadow.postgresql.Driver"
      case "redshift"  => "com.amazon.redshift.jdbc.Driver"
      case _           => "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver"
    }

    connectionProperties.setProperty("driver", dbDriver)
    connectionProperties.put("user", dxrConn.DB_USER_NAME)
    connectionProperties.put("password", dxrConn.DB_PASSWORD)

    if (dxrConn.FINAL_SESSION_SETUP_CALL.nonEmpty) {
      connectionProperties.setProperty("vpdCall", dxrConn.FINAL_SESSION_SETUP_CALL)
    }
    connectionProperties
  }

  /**
   * performs jdbc calls and returns an iterator on the result set.
   * jdbc to catalyst Internal row conversion code is a copy of compute method from org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
   *
   * @param jdbcObj
   * @param jdbcSchema
   * @return
   */
  def getDatasetIterator(
      jdbcObj: JDBCObject,
      jdbcSchema: StructType): Iterator[InternalRow] = {

    lazy val rs = jdbcObj.resultSet
    val dataRowIterator: Iterator[Row] =
      JdbcUtils.resultSetToRows(rs, jdbcSchema)
    val encoder: ExpressionEncoder[Row] = RowEncoder(jdbcSchema).resolveAndBind()
    val toRow = encoder.createSerializer()
    dataRowIterator.map(toRow)
  }

  def getSchemaFromDXRConnection(
      dxrConn: DXRConnection,
      dxrProductName: String): StructType = {
    val connectionProperties =
      getConnectionPropertiesFromDXRConnection(dxrConn, dxrProductName)

    logger.info(s"\n(${dxrConn.FINAL_SQL} \n) alias ")

    // alias is compulsory in few rdbms db like sql server
    val jdbcOptions = new JDBCOptions(
      dxrConn.JDBC_URL,
      s"\n(${dxrConn.FINAL_SQL} \n) alias ",
      connectionProperties.asScala.toMap)
    JDBCRDD.resolveTable(jdbcOptions)
  }

  /**
   * @param applicationId
   * @param attemptId
   * @return
   */
  @scala.annotation.tailrec
  def writeToHDFS(
      dxrConn: DXRConnection,
      dxrProductName: String,
      parquetCatalystWriter: ParquetWriter[InternalRow],
      filePath: String,
      jdbcSchema: StructType,
      applicationId: String,
      attemptId: Option[String],
      dxrLoggingConnectionProps: Properties,
      dxrLoggingJdbcUrl: String,
      fetchSize: Int         = 1000,
      maxRows: Int           = 0,
      maxTimeOut: Int        = 1800,
      retryIntervalSecs: Int = 60,
      extractAttemptId: Int  = 1): DXRTaskLog = {

    Logger
      .getLogger(
        "org.apache.spark.sql.execution.datasources.parquet.CatalystWriteSupport")
      .setLevel(Level.WARN)
    Logger
      .getLogger("org.apache.parquet.hadoop.InternalParquetRecordWriter")
      .setLevel(Level.DEBUG) // Debug logging to check memory usage

    val taskContext = org.apache.spark.TaskContext.get()
    val (taskAttemptNumber: Int, taskAttemptId: Long) = if (taskContext != null) {
      (taskContext.attemptNumber(), taskContext.taskAttemptId())
    } else (0, 0.toLong)

    val t0    = System.currentTimeMillis()
    var count = 0
    try {
      logger.info(
        s"DXR_CONNECTION: opening connection for ${dxrConn.DB_USER_NAME} with ${dxrConn.JDBC_URL}")
      lazy val jdbcObj =
        getJDBCObjectFromDXRConnection(
          dxrConn,
          dxrProductName,
          fetchSize,
          maxRows,
          maxTimeOut)

      // TODO: put a live status here
      val dxrStartLog = DXRTaskLog(
        APPLICATION_ID           = applicationId,
        ATTEMPT_ID               = attemptId,
        SQL_NAME                 = dxrConn.SQL_NAME,
        TARGET_KEY               = dxrConn.TARGET_KEY.toInt,
        DB_PARTITION             = dxrConn.DB_SCHEMA,
        FINAL_SELECT_SQL         = dxrConn.FINAL_SQL,
        FINAL_SESSION_SETUP_CALL = dxrConn.FINAL_SESSION_SETUP_CALL,
        BATCH_NUMBER             = dxrConn.BATCH_NUMBER,
        EXTRACT_STATUS           = "STARTED",
        EXTRACT_STATUS_CODE      = 1,
        MSG                      = null,
        TASK_ATTEMPT_NUMBER      = taskAttemptNumber,
        TASK_ATTEMPT_ID          = taskAttemptId,
        EXTRACT_ATTEMPT_ID       = extractAttemptId)

      logExtractStatus(dxrStartLog, dxrLoggingConnectionProps, dxrLoggingJdbcUrl)

      try {
        val rows = getDatasetIterator(jdbcObj, jdbcSchema)
        rows.foreach({ x =>
          parquetCatalystWriter.write(x)
          count = count + 1
          if (count % 2000 == 0) {
            logger.info(
              s"""wrote $count rows for ${dxrConn.SQL_NAME} + ( Client = ${dxrConn.CLIENT_IDENTIFIER}, extractAttemptId =$extractAttemptId , DBSCHEMA = ${dxrConn.DB_SCHEMA}) parquet filepath: $filePath""")
          }
        })

        logger.info(
          s"Completed writing " + count + s" rows in total during extractAttemptId =$extractAttemptId for " + dxrConn.SQL_NAME + "(" + dxrConn.CLIENT_IDENTIFIER + ") " + filePath)

      } finally {
        jdbcObj.closeSafely(dxrConn.DB_USER_NAME, dxrConn.JDBC_URL)
      }

      val execTime = System.currentTimeMillis() - t0
      val dxrLog = DXRTaskLog(
        APPLICATION_ID           = applicationId,
        ATTEMPT_ID               = attemptId,
        SQL_NAME                 = dxrConn.SQL_NAME,
        TARGET_KEY               = dxrConn.TARGET_KEY.toInt,
        DB_PARTITION             = dxrConn.DB_SCHEMA,
        FINAL_SELECT_SQL         = dxrConn.FINAL_SQL,
        FINAL_SESSION_SETUP_CALL = dxrConn.CLIENT_IDENTIFIER,
        BATCH_NUMBER             = dxrConn.BATCH_NUMBER,
        MSG                      = null,
        TASK_ATTEMPT_NUMBER      = taskAttemptNumber,
        TASK_ATTEMPT_ID          = taskAttemptId,
        EXEC_TIME_MS             = execTime.toInt,
        ROW_COUNT                = count)

      logExtractStatus(dxrLog, dxrLoggingConnectionProps, dxrLoggingJdbcUrl)
      dxrLog

    } catch {
      case e: Exception => {
        // TODO: put a live status here
        logger.error(
          s"Error retrieving data (rowcount = $count) for ${dxrConn.SQL_NAME} for client + ${dxrConn.CLIENT_IDENTIFIER} from schema ${dxrConn.DB_SCHEMA} because of - ${e.getMessage}")
        e.printStackTrace()

        if ((e.getMessage.contains("ORA-01652") || e.getMessage.contains(
            "ORA-01013") || e.getMessage.contains("ORA-12805") || e.getMessage.contains(
            "ORA-01555") || e.getMessage.contains("ORA-02063") || e.getMessage.contains(
            "ORA-00600") || e.getMessage
            .contains("IO Error: Socket read timed out")) && extractAttemptId < 2) {

          // TODO: put a live status here
          val dxrLog = DXRTaskLog(
            APPLICATION_ID           = applicationId,
            ATTEMPT_ID               = attemptId,
            SQL_NAME                 = dxrConn.SQL_NAME,
            TARGET_KEY               = dxrConn.TARGET_KEY.toInt,
            DB_PARTITION             = dxrConn.DB_SCHEMA,
            FINAL_SELECT_SQL         = dxrConn.FINAL_SQL,
            FINAL_SESSION_SETUP_CALL = dxrConn.FINAL_SESSION_SETUP_CALL,
            BATCH_NUMBER             = dxrConn.BATCH_NUMBER,
            MSG = e.getMessage + (if (!e.getMessage.contains("ORA")) {
                                    "\n" + e.getStackTraceString
                                  } else ""),
            TASK_ATTEMPT_NUMBER = taskAttemptNumber,
            TASK_ATTEMPT_ID     = taskAttemptId,
            EXTRACT_ATTEMPT_ID  = extractAttemptId,
            EXEC_TIME_MS        = (System.currentTimeMillis() - t0).toInt,
            IS_ERROR            = true)

          logExtractStatus(dxrLog, dxrLoggingConnectionProps, dxrLoggingJdbcUrl)

          logger.warn(
            s"retrying extract in 1 min for ${dxrConn.SQL_NAME} for client + ${dxrConn.CLIENT_IDENTIFIER} from schema ${dxrConn.DB_SCHEMA} because of - ${e.getMessage}")
          // wait before retrying
          // sleep time has to specified in Milliseconds
          Thread.sleep(retryIntervalSecs * 1000)

          writeToHDFS(
            dxrConn                   = dxrConn,
            dxrProductName            = dxrProductName,
            parquetCatalystWriter     = parquetCatalystWriter,
            filePath                  = filePath,
            jdbcSchema                = jdbcSchema,
            applicationId             = applicationId,
            attemptId                 = attemptId,
            dxrLoggingConnectionProps = dxrLoggingConnectionProps,
            dxrLoggingJdbcUrl         = dxrLoggingJdbcUrl,
            fetchSize                 = fetchSize,
            maxRows                   = maxRows,
            maxTimeOut                = maxTimeOut,
            retryIntervalSecs         = retryIntervalSecs,
            extractAttemptId          = extractAttemptId + 1)

        } else {
          val execTime = System.currentTimeMillis() - t0
          val dxrLog = DXRTaskLog(
            APPLICATION_ID           = applicationId,
            ATTEMPT_ID               = attemptId,
            SQL_NAME                 = dxrConn.SQL_NAME,
            TARGET_KEY               = dxrConn.TARGET_KEY.toInt,
            DB_PARTITION             = dxrConn.DB_SCHEMA,
            FINAL_SELECT_SQL         = dxrConn.FINAL_SQL,
            FINAL_SESSION_SETUP_CALL = dxrConn.FINAL_SESSION_SETUP_CALL,
            BATCH_NUMBER             = dxrConn.BATCH_NUMBER,
            MSG = e.getMessage + (if (!e.getMessage.contains("ORA")) {
                                    "\n" + e.getStackTraceString
                                  } else ""),
            TASK_ATTEMPT_NUMBER = taskAttemptNumber,
            TASK_ATTEMPT_ID     = taskAttemptId,
            EXTRACT_ATTEMPT_ID  = extractAttemptId,
            EXEC_TIME_MS        = execTime.toInt,
            IS_ERROR            = true)

          // TODO: put a live status here
          logExtractStatus(dxrLog, dxrLoggingConnectionProps, dxrLoggingJdbcUrl)
          dxrLog
        }

      }
    }
  }

  def logExtractStatus(
      taskLog: DXRTaskLog,
      dxrConnectionProps: Properties,
      dxrUrl: String): Boolean = {
    lazy val conn =
      getDXRLoggerConnection(dxrUrl, extractLiveLogsTable, dxrConnectionProps)
    lazy val stmt       = getDXRTaskLiveLogPreparedStmt(conn, taskLog)
    lazy val jdbcObject = JDBCObject(conn, stmt, null)
    try {
      stmt.execute()
    } catch {
      case e: Exception => {
        logger.error("DXR_EXTRACT_LIVE_LOGGING: failure while inserting logInfo ...", e)
        false
      }
    } finally {
      jdbcObject.closeSafely("dxr", dxrUrl)
    }
  }

}
