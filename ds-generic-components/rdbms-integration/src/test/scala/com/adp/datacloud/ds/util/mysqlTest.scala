package com.adp.datacloud.ds.util
import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

object mysqlTest {

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
          .appName("MySQL")
          .master("local[*]")
          .config("spark.eventLog.enabled", true)
          .config("spark.eventLog.dir", eventLogDir)
          .getOrCreate()

      } else {
        SparkSession
          .builder()
          .appName("MySQL")
          .enableHiveSupport()
          .getOrCreate()
      }

    val jdbcHostname = "cdldfosdbc05s01.es.ad.adp.com"
    val jdbcPort     = 3306
    val jdbcDatabase = "archdb"
    val jdbcUsername = "cd_relv_load"
    val jdbcPassword = "adpADP11"

    val sessionTestSql = s"""(SELECT 1 AS NUM, 2 AS NAME FROM DUAL) alias"""

    // Create the JDBC URL without passing in the user and password parameters.

    // val dsn = "(DESCRIPTION = (ADDRESS_LIST = (LOAD_BALANCE = off) (FAILOVER = ON) (ADDRESS = (PROTOCOL = TCP)(HOST = RUNFLDDB7A.es.ad.adp.com)(PORT = 1433))) (CONNECT_DATA = (database=isbsForms2600)))"

    val dsn =
      s"$jdbcHostname:3306/$jdbcDatabase?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

    val jdbcUrl = s"jdbc:mysql://${dsn}"
    println(jdbcUrl)

    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val driverClass = "com.mysql.cj.jdbc.Driver"

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
