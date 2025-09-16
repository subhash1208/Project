package com.adp.datacloud.ds.util
import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

object sqlServer {

  def main(args: Array[String]): Unit = {

    println("Text")

    implicit val sparkSession =
      if (System
          .getProperty("os.name")
          .toLowerCase()
          .contains("windows") || System
          .getProperty("os.name")
          .toLowerCase()
          .startsWith("mac")) {
        // log events to view from local history server
        val eventLogDir = s"/tmp/spark-events/"
        scala.reflect.io.Path(eventLogDir).createDirectory(true, false)

        // use local mode for testing in windows
        SparkSession
          .builder()
          .appName("SqlServer")
          .master("local[*]")
          .config("spark.eventLog.enabled", true)
          .config("spark.eventLog.dir", eventLogDir)
          .getOrCreate()

      } else {
        SparkSession
          .builder()
          .appName("SqlServer")
          .enableHiveSupport()
          .getOrCreate()
      }

    val jdbcHostname = "RUNFLDDB7A.es.ad.adp.com"
    val jdbcPort     = 1433
    val jdbcDatabase = "isbsForms2600"
    val jdbcUsername = "DataCloudExtraction"
    val jdbcPassword = "AB1tW@arm3r"

    val sessionTestSql = s"""
                (SELECT CASE transaction_isolation_level
          WHEN 0 THEN 'Unspecified'
          WHEN 1 THEN 'ReadUncommitted'
          WHEN 2 THEN 'ReadCommitted'
          WHEN 3 THEN 'Repeatable'
          WHEN 4 THEN 'Serializable'
          WHEN 5 THEN 'Snapshot' END AS TRANSACTION_ISOLATION_LEVEL,
          '[OWNERID]' AS db_schema
          FROM sys.dm_exec_sessions where session_id = @@SPID) alias
      """

    // Create the JDBC URL without passing in the user and password parameters.

    // val dsn = "(DESCRIPTION = (ADDRESS_LIST = (LOAD_BALANCE = off) (FAILOVER = ON) (ADDRESS = (PROTOCOL = TCP)(HOST = RUNFLDDB7A.es.ad.adp.com)(PORT = 1433))) (CONNECT_DATA = (database=isbsForms2600)))"

    val dsn = "RUNFLDDB7A.es.ad.adp.com:1433;databaseName=isbsForms2600"

    //val dsn ="(Host=RUNFLDDB7A.es.ad.adp.com)((PORT1433)(databaseName=isbsForms2600)"

    //val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

    val jdbcUrl = s"jdbc:sqlserver://${dsn}"

    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    //val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val driverClass = "com.microsoft.jdbc.sqlserver.SQLServerDriver"

    connectionProperties.setProperty("Driver", driverClass)

    val jdbcOptions =
      new JDBCOptions(jdbcUrl, sessionTestSql, connectionProperties.asScala.toMap)

    val connection = JdbcUtils.createConnectionFactory(jdbcOptions)()
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED)
    //    connection.setTransactionIsolation(1)
    //    println(connection.prepareCall("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").executeUpdate());

    val employees_table =
      sparkSession.read.jdbc(jdbcUrl, sessionTestSql, connectionProperties)

    employees_table.show()

    println(s"select * \n from  ADHOC_FIELD_METADATA \n where 1=2")

  }
}
