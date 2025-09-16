package com.adp.datacloud.ds.rdbms

import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j.Logger

import scala.collection.parallel.ForkJoinTaskSupport
import scala.io.Source

object parallelSqlExecutor {
  
  println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")
  
  private val logger = Logger.getLogger(getClass)

  private case class ConnectionDetails(
    TARGET_KEY:         String,
    CONNECTION_ID:      String,
    DSN:                String,
    PASSWORD:           String,
    OWNER_ID:           String = null,
    CLIENT_IDENTIFIER:  String = null,
    SESSION_SETUP_CALL: String = null,
    ADPR_EXTENSION:     String = null)

  val sessionSetupCall = s"call sp_set_vpd_ctx('[CLIENTID]','queryonly')"
  val jdbcPrefix: String = "jdbc:oracle:thin"
  val jdbcDriver: String = "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver"

  def main(args: Array[String]) {
    val config = com.adp.datacloud.cli.parallelSqlExecutorOptions.parse(args)

    val connectionLinesList = Source.fromFile(config.connFile).getLines.toList //.par

    val connectionParllelisedList = connectionLinesList.indices.map(i => {
      val values = connectionLinesList(i).split(",").map(_.trim)
      values.length match {
        case 7 => ConnectionDetails(values(0), values(1), s"${values(2)}:${config.sourceDatabasePort}/${values(3)}", values(4), values(5), values(6), sessionSetupCall)
        case 6 => ConnectionDetails(values(0), values(1), s"${values(2)}:${config.sourceDatabasePort}/${values(3)}", values(4), values(5))
        case 5 => ConnectionDetails(values(0), values(1), s"${values(2)}:${config.sourceDatabasePort}/${values(3)}", values(4))
        case _ => null
      }
    }).par

    connectionParllelisedList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(config.maxParallelThreads))
    val sourceFile = Source.fromFile(config.sqlFile)
    val sqlString = try sourceFile.mkString finally sourceFile.close()

    // Substitute sqlconf variables. Note that this is substitution and not parameter binding
    val plSqlCode = config.inputParams.foldLeft(sqlString) { (y, x) =>
      y.replaceAll("&" + x._1, x._2)
    }

    //Execute the given sql's for each connection in parallel
    val result = connectionParllelisedList.map { dbConn =>
      val sourceDatabaseUser = dbConn.CONNECTION_ID
      val sourceDatabasePassword = dbConn.PASSWORD
      val dataSourceName = dbConn.DSN
      val targetSchema = dbConn.OWNER_ID
      val vpdCall = dbConn.SESSION_SETUP_CALL
      val targetKey = dbConn.TARGET_KEY

      val finalPlSql = if (targetSchema == null || targetSchema.trim().isEmpty) plSqlCode else plSqlCode.replace("[OWNERID]", targetSchema)
      val connectionString = if (dataSourceName.toLowerCase().contains(jdbcPrefix)) dataSourceName else s"${jdbcPrefix}:@${dataSourceName}"

      val connectionProperties = new Properties()
      connectionProperties.setProperty("driver", jdbcDriver)
      connectionProperties.put("user", s"${sourceDatabaseUser}")
      connectionProperties.put("password", s"${sourceDatabasePassword}")

      try {
        val conn = DriverManager.getConnection(connectionString, connectionProperties)
        val callableStatement = conn.prepareCall(finalPlSql)
        logger.info("*******")
        logger.info(finalPlSql)
        logger.info("*******")
        callableStatement.execute() // We dont capture any dbms output for now.
        logger.info(s"Successfully Executed ${config.sqlFile} on connection ${connectionString} and ${sourceDatabaseUser} schema.")
        s"Successfully Executed ${config.sqlFile} on connection ${connectionString} and ${sourceDatabaseUser} schema."
      } catch {
        case e: Exception => {
          logger.error(e.getMessage)
          e.printStackTrace()
          logger.error(s"Execution of ${config.sqlFile} failed on connection ${connectionString} and ${sourceDatabaseUser} schema. ERROR: ${e.getMessage}")
          s"Execution of ${config.sqlFile} failed on connection ${connectionString} and ${sourceDatabaseUser} schema. ERROR: ${e.getMessage}"
        }
      }
    }

    scala.reflect.io.Path(config.logFile).createFile().printlnAll(result.toList: _*)

    val failureMessages = result.toList.filter(x => x.contains("Failed"))
    require(failureMessages.isEmpty, "PL/SQL execution failed on one or more connections... \n" + failureMessages.mkString(" \n"))

  }

}