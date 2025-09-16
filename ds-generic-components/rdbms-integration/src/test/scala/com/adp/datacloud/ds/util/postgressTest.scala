package com.adp.datacloud.ds.util
import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.adp.datacloud.ds.rdbms.dxrParallelDataExporter
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

object postgressTest {

  private val logger = Logger.getLogger(getClass())

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

    val jdbcHostname =
      "aurora-cluster-adpa-instance.ccpascqtyva8.us-east-1.rds.amazonaws.com"
    val jdbcPort     = 5432
    val jdbcDatabase = "adpadb"
    val jdbcUsername = "rtw"
    val jdbcPassword = "adpadp2^tYdE$wqp"

    val sessionTestSql = s"""(select * from t_associates limit 10) alias"""

    // Create the JDBC URL without passing in the user and password parameters.

    // val dsn = "(DESCRIPTION = (ADDRESS_LIST = (LOAD_BALANCE = off) (FAILOVER = ON) (ADDRESS = (PROTOCOL = TCP)(HOST = RUNFLDDB7A.es.ad.adp.com)(PORT = 1433))) (CONNECT_DATA = (database=isbsForms2600)))"

    val dsn = s"$jdbcHostname:$jdbcPort/$jdbcDatabase"

    val jdbcUrl = s"jdbc:postgresql://${dsn}"

    println(jdbcUrl)

    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val driverClass = "org.shadow.postgresql.Driver"

    connectionProperties.setProperty("driver", driverClass)

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

    println(jdbcUrl)
    println("connectionProperties is ")
    println(connectionProperties)
    println("connectionProperties succes ")
    val conn = DriverManager.getConnection(jdbcUrl, connectionProperties)

    println("#2")
    val username = connectionProperties.get("user")
    println("#3")

    val plsqlString = "Truncate table stg_rtw_ev5_dim_orgn"

    val startTime = System.currentTimeMillis()

    /*
    try {
      val callableStatement = conn.createStatement()

      println("Trying to Truncate table " + plsqlString)
      callableStatement.executeUpdate(plsqlString)
      println("table Truncated")

    }
     */

    println("Calling pgsql")
    // val x = dxrParallelDataExporter.runPgsql(plsqlString,jdbcUrl, connectionProperties,"PRE","SomeTable")

    val x =
      dxrParallelDataExporter.runPlsql(plsqlString, jdbcUrl, connectionProperties, "PRE")

    println("Calling success")

  }

  /* Created Separate Method for Postgress Pre and post statement execution
   * 1. In Postgress even DDL are considered as DML so we need to have transaction control statement
   * 2. Pgsql has different syntax , so it gives flexibily to change the code in future.

   */
  def runPgsql(
      plsqlString: String,
      connectionString: String,
      connectionProperties: Properties,
      step: String,
      targetTable: String): Long = {

    //Class.forName("org.shadow.postgresql.Driver").newInstance();

    connectionProperties.setProperty("driver", "org.shadow.postgresql.Driver")

    val jdbcOptions =
      new JDBCOptions(connectionString, targetTable, connectionProperties.asScala.toMap)
    val connection =
      JdbcUtils.createConnectionFactory(jdbcOptions)()

    val username = connectionProperties.get("user")

    logger.info(
      s"DXR_CONNECTION: $step SQL opened connection for $username with $connectionString")
    val startTime = System.currentTimeMillis()
    logger.info(s"*** $step SQL STARTED for $username ****")

    try {
      val callableStatement = connection.createStatement()
      callableStatement.executeUpdate(plsqlString)

      // Intentionally kept in try catch because we dont know database is auto commit or not.
      // In Postgress sql DDL also needs commit;
      try {
        connection.commit();
      } catch {
        case e: Exception =>
          logger.info(
            "Exception while commit " + e.getMessage + " This is expected and ignored")
      }

      logger.info(s"Successfully Executed plsql on connection $connectionString.")

    } finally {
      connection.close()
      logger.info(s"DXR_CONNECTION: $step SQL closed connection for ${connectionProperties
        .get("user")} with $connectionString")
    }

    val execTime = System.currentTimeMillis() - startTime
    logger.info(s"*** $step SQL FINISHED for $username in $execTime ms ****")
    execTime
  }
}
