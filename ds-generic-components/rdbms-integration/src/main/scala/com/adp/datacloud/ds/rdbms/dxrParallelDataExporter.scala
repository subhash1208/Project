package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{ExporterTaskLog, DxrParallelDataExporterConfig}
import com.adp.datacloud.ds.aws.S3Utils
import com.adp.datacloud.ds.udfs.UDFUtility
import com.adp.datacloud.ds.util.{DXR2Exception, DXRLogger, DxrUtils}
import com.adp.datacloud.ds.utils.dxrCipher
import org.apache.log4j.Logger
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import java.io.{PrintWriter, StringWriter}
import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.Executors
import scala.collection.JavaConverters.propertiesAsScalaMapConverter

object dxrParallelDataExporter {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val dxrConfig =
      com.adp.datacloud.cli.dxrParallelDataExporterOptions.parse(args)

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(dxrConfig.applicationName)
      .enableHiveSupport()
      .getOrCreate()

    val filesString = dxrConfig.files
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

    runExport(dxrConfig)
  }

  def runExport(config: DxrParallelDataExporterConfig)(implicit
                                                       sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext

    import sparkSession.implicits._
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    logger.info("DXR_CONNECTION_STRING= " + {
      config.dxrJdbcUrl
    })
    logger.info("DXR_SQL_STRING = " + config.dxrSql)

    logger.debug(write(sc.getConf.getAll))

    val dxrLogger =
      DXRLogger(config, List(config.hiveSql), config.inputParams.getOrElse("env", ""))

    dxrLogger.insertStartStatusLog(None)

    try {

      logger.info("HQL_STRING= " + config.hiveSql)

      // Prepare hive table Dataframe
      val hiveDfColumns: Array[String] =
        sparkSession.sql(config.hiveSql).columns.map(_.toLowerCase())

      // Filter by hive partition column
      val filteredHql = config.hivePartitionColumn match {
        case Some(x) =>
          if (hiveDfColumns.contains(x.toLowerCase())) {
            s"select * from (${config.hiveSql})  where  $x= '${config.hivePartitionValue.getOrElse("")}' "
          } else {
            logger.warn(s"EXPORT_CONFIG: config partition col $x does not exist in table")
            config.hiveSql
          }
        case None => config.hiveSql
      }

      logger.info("FILTERED_HQL_STRING= " + filteredHql)

      val dxrConnDf: DataFrame = getDXRConnectionsDf(config)

      val dsns = dxrConnDf.select("DSN").distinct.collect().flatMap(_.toSeq)
      val byDsnArray =
        dsns.map(dsn => dxrConnDf.where($"DSN" <=> dsn).collect()).par

      val logDf =
        byDsnArray
          .flatMap(connArray => exportData(connArray, config, hiveDfColumns, filteredHql))
          .toList
          .toDF()
          .drop("JDBC_URL")

      dxrLogger.writeExportTasksLogs(logDf)

      val connectionErrorsCount = logDf.filter($"IS_ERROR" === true).count
      // log job as failure if there were errors during export
      if (connectionErrorsCount > 0) {
        throw DXR2Exception(s"CONNECTION_ERROR_COUNT=$connectionErrorsCount")
      } else {
        dxrLogger.updateStatusLog(None, "FINISHED", 2, "")
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        dxrLogger.updateStatusLog(None, "FAILED", 4, sw.toString, isError = true)
        // rethrow the error to mark the job as failure
        throw e
    }
  }

  def getDXRConnectionsDf(config: DxrParallelDataExporterConfig)(implicit
                                                                 sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val decrypt_udf = udf((pwd: String) => {
      dxrCipher.decrypt(pwd)
    })

    val preSql = config.preExportSql match {
      case Some(sqlFileName) => config.getSqlFromFile(sqlFileName)
      case None => s""
    }

    val postSql = config.postExportSql match {
      case Some(sqlFileName) => config.getSqlFromFile(sqlFileName)
      case None => s""
    }

    val stagingTable = if (config.useStagingTable) {
      config.stagingTableName match {
        case Some(stagingTableName) => stagingTableName
        case None => s"${config.targetTable}_STG"
      }
    }
    else s""

    val dxrConnDf = sparkSession.read
      .jdbc(config.dxrJdbcUrl, config.dxrSql,
        config.dxrConnectionProperties)
      .withColumn(
        "TARGET_KEY",
        $"TARGET_KEY"
          .cast(IntegerType)
      ) // convert float values to string values of int
      .withColumn(
        "DSN",
        (config.jdbcDBType match {
          case "oracle" =>
            lower(
              regexp_replace($"DSN", "\\s+", "")
            ) // oracle is case insensitive w.r.t to DNS
          case _ => regexp_replace($"DSN", "\\s+", "")
        }))
      .withColumn("PASSWORD", decrypt_udf($"CONNECTION_PASSWORD"))
      .withColumn(
        "OWNER_ID",
        when($"OWNER_ID".isNull, lit("")).otherwise(trim($"OWNER_ID")))
      .withColumn(
        "PRE_SQL",
        when($"OWNER_ID" === "", lit(preSql))
          .otherwise(regexp_replace(lit(preSql), lit(s"\\[OWNERID\\]"), $"OWNER_ID")))
      .withColumn(
        "PRE_SQL",
        when($"CONNECTION_ID" === "", col("PRE_SQL"))
          .otherwise(
            regexp_replace(col("PRE_SQL"), lit(s"\\[CONNECTIONID\\]"), $"CONNECTION_ID")))
      .withColumn(
        "POST_SQL",
        when($"OWNER_ID" === "", lit(postSql))
          .otherwise(regexp_replace(lit(postSql), lit(s"\\[OWNERID\\]"), $"OWNER_ID")))
      .withColumn(
        "POST_SQL",
        when($"CONNECTION_ID" === "", col("POST_SQL"))
          .otherwise(
            regexp_replace(
              col("POST_SQL"),
              lit(s"\\[CONNECTIONID\\]"),
              $"CONNECTION_ID")))
      .withColumn(
        "TARGET_TABLE",
        when($"OWNER_ID" === "", lit(config.targetTable)).otherwise(
          regexp_replace(lit(config.targetTable), lit(s"\\[OWNERID\\]"), $"OWNER_ID")))
      .withColumn(
        "STG_TABLE",
        when($"OWNER_ID" === "", lit(stagingTable)).otherwise(
          regexp_replace(lit(stagingTable), lit(s"\\[OWNERID\\]"), $"OWNER_ID")))
      .withColumn(
        "CONN_STRING",
        when($"DSN".startsWith("jdbc"), $"DSN")
          .otherwise(
            concat(
              lit(config.jdbcPrefix +
                (config.jdbcDBType match {
                  case "oracle" => ":@"
                  case _ => "://" // Everything else
                })),
              $"DSN")))

    dxrConnDf
  }

  def exportData(
                  connList: Array[Row],
                  config: DxrParallelDataExporterConfig,
                  rawdfColumns: Array[String],
                  filteredHql: String)(implicit sparkSession: SparkSession): List[ExporterTaskLog] = {
    val sc = sparkSession.sparkContext

    // register all customs udfs
    UDFUtility.registerUdfs

    val logs: Array[ExporterTaskLog] = connList.flatMap { conn =>

      val databaseUser: String = conn.getAs("CONNECTION_ID")
      val databasePassword: String = conn.getAs("PASSWORD")
      val databaseDNS: String = conn.getAs("DSN")
      val targetSchema: String = conn.getAs[String]("OWNER_ID")
      val targetKey = conn.getAs[Integer]("TARGET_KEY")
      val targetTable = conn.getAs[String]("TARGET_TABLE")
      val stagingTable = conn.getAs[String]("STG_TABLE")
      val exportSchema = targetSchema
      val clientId = conn.getAs[String]("CLIENT_IDENTIFIER")
      val connectionString = conn.getAs[String]("CONN_STRING")
      val db_schema =
        DxrUtils.getDbSchema(targetSchema, databaseDNS, databaseUser)

      val preSql = conn.getAs[String]("PRE_SQL")
      val postSql = conn.getAs[String]("POST_SQL")

      val connectionProperties = new Properties()
      connectionProperties.setProperty("driver", config.jdbcDriver)
      connectionProperties.put("user", s"$databaseUser")
      connectionProperties.put("password", s"$databasePassword")
      connectionProperties.put("db_schema", s"$db_schema")
      connectionProperties
        .setProperty("plsqlTimeout", config.plsqlConnectionTimeOut.toString)

      logger.info("CONNECTION_STRING= " + connectionString)

      // execute pre sql

      val preExportLog = if (preSql.nonEmpty) {
        try {
          connectionProperties.setProperty("driver", config.jdbcDriver)
          connectionProperties
            .setProperty("plsqlTimeout", config.plsqlConnectionTimeOut.toString)
          val execTime =
            runPlsql(preSql, connectionString, connectionProperties, "PRE")
          Some(
            ExporterTaskLog(
              sc.applicationId,
              sc.applicationAttemptId,
              targetKey,
              exportSchema,
              databaseUser,
              connectionString,
              targetSchema,
              clientId,
              preSql,
              null,
              s"finished preSql",
              execTime.toInt))
        } catch {
          case e: Exception => {
            logger
              .error(s"DXR_EXPORT_PRE: error while executing pre export sql", e)
            Some(
              ExporterTaskLog(
                sc.applicationId,
                sc.applicationAttemptId,
                targetKey,
                exportSchema,
                databaseUser,
                connectionString,
                targetSchema,
                clientId,
                preSql,
                null,
                s"error while running preSql - ${e.getMessage}",
                IS_ERROR = true)
            )
          }
        }

      } else None

      // check if target table exists
      val validateTableLog =
        if (preExportLog.nonEmpty && preExportLog.get.IS_ERROR) {
          None
        } else {
          try {
            if (checkTableExists(targetTable, connectionString, connectionProperties)) {
              Some(
                ExporterTaskLog(
                  sc.applicationId,
                  sc.applicationAttemptId,
                  targetKey,
                  exportSchema,
                  databaseUser,
                  connectionString,
                  targetSchema,
                  clientId,
                  targetTable,
                  null,
                  s"finished validating $targetTable table's existence"))

            } else throw new Exception(s"target table $targetTable not found ")
          } catch {
            case e: Exception =>
              logger.error(s"error checking table $targetTable", e)
              Some(
                ExporterTaskLog(
                  sc.applicationId,
                  sc.applicationAttemptId,
                  targetKey,
                  exportSchema,
                  databaseUser,
                  connectionString,
                  targetSchema,
                  clientId,
                  targetTable,
                  null,
                  e.getMessage,
                  IS_ERROR = true))
          }
        }

      // if table validation succeeds proceed to export

      val exportLog =
        if (validateTableLog.isDefined && !validateTableLog.get.IS_ERROR) {
          val metaDataSelectSql = s"(select * from $targetTable where 1=2) myalias"

          // If db_schema column exists in the hive table, filter the dataframe further with [OWNER_ID]
          implicit val finalHiveSql: String = if (rawdfColumns.contains("db_schema")) {
            // Filter the dataframe for this particular db_schema and drop it from projections
            s"select * from ($filteredHql) where db_schema = '$db_schema'"
          } else {
            // Typically happens when the same data is exported to multiple schemas
            filteredHql
          }

          val exportDf = sparkSession.sql(finalHiveSql)
          val t0 = System.currentTimeMillis()

          try {
            val warehouseDf = sparkSession.read
              .jdbc(connectionString, metaDataSelectSql, connectionProperties)

            val warehouseColumns = warehouseDf.columns.map(_.toUpperCase()).toList

            //Preparing the Dataframe with similar schema like warehouse table to export.
            val colsToRemove = exportDf.columns.map(_.toUpperCase()).toList

            // Not an elegant way of doing this but postgres sucks w.r.t to column case
            // convention in redshift is to have lowercase column names
            val commonColumns = if (config.jdbcDBType == "postgres" || config.jdbcDBType == "redshift") {
              warehouseColumns.intersect(colsToRemove).map(_.toLowerCase())
            } else {
              warehouseColumns.intersect(colsToRemove)
            }

            require(commonColumns.nonEmpty, "no common columns found to export")

            val finalDf =
              exportDf.select(commonColumns.head, commonColumns.tail: _*)

            finalDf.printSchema()

            val exportCount = finalDf.count()

            logger.info(
              s"DXR_EXPORT_COUNT: $exportCount for $databaseUser with $finalHiveSql")

            if (config.useStagingTable) {
              val jdbcOptions = new JdbcOptionsInWrite(
                connectionString,
                stagingTable,
                connectionProperties.asScala.toMap)
              val stagingCheckConn =
                JdbcUtils.createConnectionFactory(jdbcOptions)()

              if (JdbcUtils.tableExists(stagingCheckConn, jdbcOptions)) {
                // Drop staging table if it exists and recreate it. This is necessary to make sure any column changes in target also reflect in staging
                // TODO: Improve exception handling
                val dropStagingConn =
                JdbcUtils.createConnectionFactory(jdbcOptions)()
                val dropstmt =
                  stagingCheckConn.prepareStatement(s"DROP TABLE $stagingTable")
                try {
                  dropstmt.execute()
                } catch {
                  case ex: SQLException =>
                    logger.error(s"Couldn't drop existing staging table $stagingTable")
                    throw ex
                } finally {
                  dropstmt.close()
                  dropStagingConn.close()
                }
              }

              // Figure out a solution to avoid not null constraints on staging table
              val createStmt = stagingCheckConn.prepareStatement(
                s"CREATE TABLE $stagingTable AS (SELECT ${commonColumns.mkString(",")} FROM $targetTable WHERE 1 = 2)")
              try {
                createStmt.execute()
              } catch {
                case ex: Exception =>
                  logger.error(s"Couldn't create staging table $stagingTable")
                  throw ex
              } finally {
                createStmt.close()
                stagingCheckConn.close()
              }
              insertIntoTable(finalDf, jdbcOptions, config)
              //transfer data into final Table
              val connection = JdbcUtils.createConnectionFactory(jdbcOptions)()
              connection.setAutoCommit(false)

              // cannot use distinct in copy sql if the table contains a clob columns
              val copySql = config.mergeColumns match {
                case Some(ids) => {
                  val joinCondition = ids.map(x => s"t.${x} = s.${x}").mkString(" AND ")
                  val updates = commonColumns
                    .filter(x => !ids.contains(x))
                    .map(x => s"t.${x} = s.${x}")
                    .mkString(",")
                  val inserts = commonColumns
                    .filter(x => !ids.contains(x))
                    .map(x => s"t.${x}")
                    .mkString(",")
                  val values = commonColumns
                    .filter(x => !ids.contains(x))
                    .map(x => s"s.${x}")
                    .mkString(",")
                  s"""MERGE INTO $targetTable t
                      USING (SELECT ${commonColumns.mkString(",")} FROM $stagingTable) s
                        ON ${joinCondition}
                      WHEN MATCHED THEN
                        UPDATE SET ${updates}
                      WHEN NOT MATCHED THEN
                        INSERT (${inserts}) VALUES (${values}}
                  """
                }
                case None => {
                  s"insert into $targetTable (${commonColumns.mkString(",")}) select ${
                    commonColumns
                      .mkString(",")
                  } from $stagingTable"
                }
              }

              logger.info("COPY_DATA_SQL_STRING = " + copySql)
              val copyStatement = connection.prepareStatement(copySql)
              var committed = false
              try {
                logger.info(
                  s"DXR_EXPORT: starting copy from $stagingTable to $targetTable for $databaseUser with $finalHiveSql"
                )
                copyStatement.execute()
                connection.commit()
                logger.info(
                  s"DXR_EXPORT: finished copy from $stagingTable to $targetTable for $databaseUser with $finalHiveSql")
                committed = true
              } catch {
                case ex: Exception =>
                  logger.error(s"Couldn't copy from staging table $stagingTable to target table $targetTable")
                  throw ex
              } finally {
                copyStatement.close()
                if (!committed) {
                  connection.rollback()
                }
                connection.close()
              }
            } else {
              val jdbcOptions = new JdbcOptionsInWrite(
                connectionString,
                targetTable,
                connectionProperties.asScala.toMap
              )
              insertIntoTable(finalDf, jdbcOptions, config)
            }

            val execTime = System.currentTimeMillis() - t0
            Some(
              ExporterTaskLog(
                sc.applicationId,
                sc.applicationAttemptId,
                targetKey,
                exportSchema,
                databaseUser,
                connectionString,
                targetSchema,
                clientId,
                finalHiveSql,
                null,
                s"finished export to $targetTable for $databaseUser with $finalHiveSql",
                execTime.toInt,
                exportCount.toInt))

          } catch {
            case ex: Exception => {
              val execTime = System.currentTimeMillis() - t0
              val exportLog = ExporterTaskLog(
                sc.applicationId,
                sc.applicationAttemptId,
                targetKey,
                exportSchema,
                databaseUser,
                connectionString,
                targetSchema,
                clientId,
                finalHiveSql,
                null,
                s"export failed for $targetTable with error: " + ex.getMessage,
                execTime.toInt,
                IS_ERROR = true)
              logger
                .error(s"DXR_EXPORT: $exportLog export error " + ex.getMessage, ex)
              Some(exportLog)

            }
          }

        } else {
          None
        }

      val postExportLog =
        if (postSql.isEmpty || exportLog.isEmpty || exportLog.get.IS_ERROR) None
        else {
          try {
            val execTime =
              runPlsql(postSql, connectionString, connectionProperties, "POST")
            Some(
              ExporterTaskLog(
                sc.applicationId,
                sc.applicationAttemptId,
                targetKey,
                exportSchema,
                databaseUser,
                connectionString,
                targetSchema,
                clientId,
                postSql,
                null,
                s"finished postSql",
                execTime.toInt))
          } catch {
            case e: Exception => {
              logger
                .error(s"DXR_EXPORT_POST: error while executing post export sql", e)
              Some(
                ExporterTaskLog(
                  sc.applicationId,
                  sc.applicationAttemptId,
                  targetKey,
                  exportSchema,
                  databaseUser,
                  connectionString,
                  targetSchema,
                  clientId,
                  postSql,
                  null,
                  s"error while running postSql - ${e.getMessage}",
                  IS_ERROR = true))
            }
          }
        }
      // consolidate logs
      val executionLogs =
        List(preExportLog, validateTableLog, exportLog, postExportLog).flatten
      executionLogs
    }
    logs.toList
  }

  def insertIntoTable(df: DataFrame, jdbcOptions: JdbcOptionsInWrite,
                      config: DxrParallelDataExporterConfig)(implicit finalHiveSql: String): Unit = {
    val dbUser = jdbcOptions.parameters.get("user").get
    val table = jdbcOptions.table
    logger.info(s"DXR_EXPORT: starting export to $table for user $dbUser with $finalHiveSql")

    if (config.jdbcDBType.toLowerCase() == "redshift") {
      /* Redshift serializes (Serializable isolation level) concurrent write transactions by acquiring locks on a table.
        This which causes performance issues in high volume exports when queries are fired in parallel by spark.
        Using copy command to load data from S3 is recommended, so we're using the spark-redshift connector.
        See https://docs.aws.amazon.com/redshift/latest/dg/c_Concurrent_writes.html for more details. */
      val tempDir = config.redshiftS3TempDir.get
      val extraParams = Map(
        "tempdir" -> tempDir.replaceFirst("s3", "s3a"),
        "tempformat" -> "AVRO",
        "aws_iam_role" -> config.redshiftIAMRole.get
      )
      try {
        df.write
          .options(jdbcOptions.parameters ++ extraParams)
          .format("io.github.spark_redshift_community.spark.redshift")
          .mode(SaveMode.Append)
          .save()
      } catch {
        case ex: Exception =>
          logger.error(s"Failed while writing to table $table")
          throw ex
      } finally {
        S3Utils.deleteObjects(tempDir) // clean up s3 tempdir
      }
    } else {
      try {
        JdbcUtils.saveTable(
          df.repartition(config.maxParallelDBConnections),
          None,
          isCaseSensitive = false,
          jdbcOptions
        )
      } catch {
        case ex: Exception => logger.error(s"Failed while writing to table $table")
          ex.printStackTrace()
      }
    }
    logger.info(
      s"DXR_EXPORT: finished export to $table for $dbUser with $finalHiveSql"
    )
  }

  def runPlsql(plsqlString: String,
               connectionString: String,
               connectionProperties: Properties,
               step: String): Long = {

    // val conn = DriverManager.getConnection(connectionString, connectionProperties)
    // Replaced with Below block as Driver is always initialized to Oracle

    val jdbcOptions = new JDBCOptions(
      connectionString,
      "SomeProxyTable",
      connectionProperties.asScala.toMap)

    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()

    val username = connectionProperties.get("user")
    val dbSchema = connectionProperties.get("db_schema")
    val plsqlTimeOut = connectionProperties.get("plsqlTimeout").toString.toInt
    logger.info(
      s"DXR_CONNECTION: $step SQL opened connection for $username with $connectionString")
    val startTime = System.currentTimeMillis()
    logger.info(s"*** $step SQL STARTED for $dbSchema ****")
    try {
      conn.setNetworkTimeout(Executors.newSingleThreadExecutor(), plsqlTimeOut)
      val callableStatement = conn.prepareCall(plsqlString)
      logger.info(plsqlString)
      logger.info(s"PQSL_NetworkTimeout in ms:${conn.getNetworkTimeout}")
      println(callableStatement.execute()) // We dont capture any dbms output for now.
      logger.info(s"Successfully Executed plsql on connection $connectionString.")
    } finally {
      conn.close()
      logger.info(s"DXR_CONNECTION: $step SQL closed connection for ${
        connectionProperties
          .get("user")
      } with $connectionString")
    }
    val execTime = System.currentTimeMillis() - startTime
    logger.info(s"*** $step SQL FINISHED for $dbSchema in $execTime ms ****")
    execTime
  }


  def checkTableExists(targetTable: String,
                       connectionString: String,
                       connectionProperties: Properties): Boolean = {

    val jdbcOptions = new JdbcOptionsInWrite(
      connectionString,
      targetTable,
      connectionProperties.asScala.toMap)

    val jdbcConn =
      JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.tableExists(jdbcConn, jdbcOptions)
    } finally {
      jdbcConn.close()
    }
  }
}
