package com.adp.datacloud.util

import java.sql.Connection
import java.util.Properties
import com.adp.datacloud.cli.tableStatsGeneratorOptions
import oracle.jdbc.OracleConnection
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.parallel.ForkJoinTaskSupport

case class TABLE_CONFIG(
  SCHEMA_NAME:  String,
  TABLE_NAME:   String,
  SKIP_FILTERS: Boolean = false)

object tableStatsGenerator {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {
    
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val config = tableStatsGeneratorOptions.parse(args)
    val osName = System
      .getProperty("os.name")
      .toLowerCase()

    implicit val sparkSession: SparkSession =
      if (osName.contains("windows") || osName.startsWith("mac")) {
        // log events to view from local history server
        val eventLogDir = s"/tmp/spark-events/"
        scala.reflect.io
          .Path(eventLogDir)
          .createDirectory(force = true, failIfExists = false)

        // store hive tables in this location in local mode
        val warehouseLocation = s"/tmp/spark-warehouse/"
        scala.reflect.io
          .Path(warehouseLocation)
          .createDirectory(force = true, failIfExists = false)

        // use local mode for testing in windows
        SparkSession
          .builder()
          .appName(config.applicationName)
          .master("local[*]")
          .config("spark.eventLog.enabled", value = true)
          .config("spark.eventLog.dir", eventLogDir)
          .enableHiveSupport()
          .getOrCreate()

      } else {
        SparkSession
          .builder()
          .appName(config.applicationName)
          .enableHiveSupport()
          .getOrCreate()
      }

    val filesString = config.files
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

    import sparkSession.implicits._

    val jdbcConnectionProperties = new Properties()
    jdbcConnectionProperties.setProperty(
      "driver",
      "oracle.jdbc.driver.OracleDriver")
    // pass the the jdbc url including user name and password if wallet is not setup for local mode
    // jdbc:oracle:thin:<UserName>/<password>@<hostname>:<port>/<serviceID>

    if (config.oracleWalletLocation.isDefined) {
      jdbcConnectionProperties.setProperty(
        OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION,
        config.oracleWalletLocation.get)
    }
    if (config.outputDBPassword.isDefined && config.outputDBUserName.isDefined) {
      jdbcConnectionProperties.put("user", s"${config.outputDBUserName.get}")
      jdbcConnectionProperties.put(
        "password",
        s"${config.outputDBPassword.get}")
    }

    // create the metrics table

    val outputTableName = s"TABLE_METRICS"
    val outputTableOptions = new JdbcOptionsInWrite(
      config.outputDBJdbcURL,
      outputTableName,
      jdbcConnectionProperties.asScala.toMap)
    val tableConn = JdbcUtils.createConnectionFactory(outputTableOptions)()
    try {
      val tableExists = JdbcUtils.tableExists(tableConn, outputTableOptions)
      if (!tableExists) {
        val sourceFile = scala.io.Source
          .fromInputStream(
            this.getClass.getResourceAsStream(s"/createMetricsTable.sql"))
        val createTableSql =
          try sourceFile.mkString.replace("&tableName",outputTableName)
          finally sourceFile.close()
        runSqlStmtSafely(tableConn, createTableSql)
      }
    } finally {
      tableConn.close()
    }

    // delete all the existing counts for a clean rerun
    if (config.overWriteCounts) {
      val filtersString =
        config.filterConditions.mkString(" AND ").replaceAll("'", "")
      config.inputSchemas.foreach(schema => {
        val deleteRecordsSQL =
          s"delete from $outputTableName where schema_name = '$schema' AND filters = '$filtersString'"
        runSqlStmtSafely(outputTableOptions, deleteRecordsSQL)
      })
    }

    val statusString = config.inputSchemas
      .flatMap(dbname => {

        val catalogSchemaName = dbname.toLowerCase() match {
          case name if name.contains("main") => "main"
          case name if name.contains("base") => "base"
          case name if name.contains("raw")  => "raw"
          case name                          => name
        }

        val catalog_sql =
          s"(select '$dbname' as SCHEMA_NAME,TRIM(TABLE_NAME) as TABLE_NAME,TRIM(SKIP_FILTERS) as SKIP_FILTERS from SPARK_COUNT_CATALOG_TABLE where TRIM(schema_name) = '$catalogSchemaName')"

        logger.info(s"catalog_sql = $catalog_sql")

        val catalogDf = sparkSession.read
          .jdbc(config.outputDBJdbcURL, catalog_sql, jdbcConnectionProperties)
        val catalogList = if (catalogDf.isEmpty) {
          sparkSession.catalog
            .listTables(dbname)
            .collect()
            .map(x => {
              TABLE_CONFIG(x.database, x.name)
            })
            .toList
        } else {
          catalogDf.as[TABLE_CONFIG].collect().toList
        }

        val errors = generateStats(
          catalogList,
          config.filterConditions,
          config.outputDBJdbcURL,
          jdbcConnectionProperties,
          outputTableName,
          config.overWriteCounts)
        if (errors.nonEmpty)
          Some(
            s"${errors.mkString(",")} tables erred while fetching counts in $dbname.")
        else None
      })
      .mkString("")

    if (statusString.length > 0)
      throw new Exception(statusString)

  }

  def generateStats(
    tablesList:           List[TABLE_CONFIG],
    filterConditions:     List[String],
    jdbcURL:              String,
    connectionProperties: Properties,
    outputTableName:      String             = "TABLE_METRICS",
    overWriteCounts:      Boolean            = false)(
    implicit
    sparkSession: SparkSession): Seq[String] = {

    val filterConditionsMap = filterConditions
      .map(x => {
        (x.trim.split("=")(0), x.trim)
      })
      .toMap

    import sparkSession.implicits._

    val databaseName = tablesList.head.SCHEMA_NAME

    val filtersString = filterConditions.mkString(" AND ").replaceAll("'", "")

    val outputTableOptions = new JdbcOptionsInWrite(
      jdbcURL,
      outputTableName,
      connectionProperties.asScala.toMap)

    val existingCountsSQL =
      s"select TABLE_NAME from $outputTableName where schema_name = '$databaseName' AND filters = '$filtersString'"
    val existingCounts = sparkSession.read
      .jdbc(jdbcURL, s"($existingCountsSQL)", connectionProperties)
      .map(row => row.getAs[String](0))
      .collect()
      .toList

    val catalog = sparkSession.catalog
    // remove tables that had counts already computed
    val filteredTableList = tablesList.filterNot(tc => {
      existingCounts.contains(tc.TABLE_NAME)
    })
    val tableNamesList = filteredTableList.map(_.TABLE_NAME)

    val tablesListParalleled = filteredTableList.par
    tablesListParalleled.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(2))

    val errors: Seq[String] = tablesListParalleled
      .flatMap(table => {
        val tableName = table.TABLE_NAME
        val tableFullName = s"${table.SCHEMA_NAME}.${table.TABLE_NAME}"

        try {
          val columns = catalog.listColumns(tableFullName)
          val columnsList = columns
            .collect()
            .map(_.name.toLowerCase)
          val fullDF = sparkSession.table(tableFullName)
          val filteredDf = if (table.SKIP_FILTERS) {
            fullDF
          } else {
            filterConditionsMap.keys.foldLeft(fullDF) { (baseDF, key) =>
              {
                if (columnsList.contains(key)) {
                  val filterString: String = filterConditionsMap(key)
                  logger.info(
                    s"filter applied on  the table $tableFullName is $filterString")
                  baseDF.filter(s"$filterString")
                } else {
                  baseDF
                }
              }
            }
          }

          val count = filteredDf.count()
          val tableSeqNo = tableNamesList.indexOf(table.TABLE_NAME) + 1
          logger.info(
            s"$count is the row count for table $tableFullName, its $tableSeqNo in sequence out of ${filteredTableList.size}")

          val insertSql =
            s"Insert into $outputTableName (SCHEMA_NAME,TABLE_NAME,TABLE_COUNT,FILTERS) values ('$databaseName','$tableName',$count,'$filtersString')"
          runSqlStmtSafely(outputTableOptions, insertSql)

          None
        } catch {
          case e: Exception =>
            logger
              .error(
                s"Error while fetching count for table $tableFullName: ",
                e)
            Some(tableName)
        }
      })
      .toList

    errors
  }

  private def runSqlStmtSafely(conn: Connection, sqlString: String): Int = {
    val statement = conn.createStatement
    try {
      logger.info(s"Running Sql: sqlString")
      statement.executeUpdate(sqlString)
    } finally {
      statement.close()
    }
  }

  private def runSqlStmtSafely(options: JDBCOptions, sqlString: String): Int = {
    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      runSqlStmtSafely(conn, sqlString)
    } catch {
      case e: Exception =>
        logger
          .error(s"error while running the sql : $sqlString", e)
        0
    } finally {
      conn.close()
    }
  }

}
