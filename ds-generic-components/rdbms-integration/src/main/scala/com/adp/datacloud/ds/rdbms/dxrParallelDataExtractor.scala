package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{DxrParallelDataExtractorConfig, dxrParallelDataExtractorOptions}
import com.adp.datacloud.ds.DXRDataExtractor
import  com.adp.datacloud.ds.rdbms.dxrDataWriter.hashUdf
import com.adp.datacloud.ds.util._
import com.adp.datacloud.writers.HiveDataFrameWriter
import com.adp.datacloud.ds.hive.hiveTableUtils
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.Encoders
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import com.adp.datacloud.writers.DeltaDataFrameWriter

import java.io.{PrintWriter, StringWriter}
import java.net.URI
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.Option
import io.delta.tables._


object dxrParallelDataExtractor extends DXRDataExtractor {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val dxrConfig: DxrParallelDataExtractorConfig =
      dxrParallelDataExtractorOptions.parse(args)

    Logger
      .getLogger(
        "org.apache.spark.sql.execution.datasources.parquet.CatalystWriteSupport")
      .setLevel(Level.WARN)

    implicit val sparkSession: SparkSession =
      SparkSession
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

    runExtract(dxrConfig)
  }

  def runExtract(config: DxrParallelDataExtractorConfig)(implicit
      sparkSession: SparkSession): Unit = {

    System.getProperties.asScala.foreach(println)
    System.currentTimeMillis()

    logger.info("DXR_CONNECTION_STRING= " + {
      config.dxrJdbcUrl
    })
    logger.info("DXR_SQL_STRING = " + config.dxrSql)

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    logger.debug(write(sparkSession.conf.getAll))

    val dxrLogger =
      DXRLogger(config, config.sqlFileNames, config.inputParams.getOrElse("env", ""))

    dxrLogger.insertStartStatusLog(Some(config.landingDatabaseName))

    try {
      val incrementalDates: Option[Dataset[DXRIncrementStatus]] =
        if (config.isManagedIncremental) {
          dxrLogger.getIncrementalDateBySqlNames(config, config.sqlFileNames)
        } else {
          None
        }
      val connFinalDS = getConnectionsDfFromConfig(config, incrementalDates)

      val batchedConnDS = config.batchColumn match {
        case Some(x) => {
          getBatchedConnectionsDF(x, config.numBatches, connFinalDS)
        }
        case _ => connFinalDS
      }

      extractDataByConnections(
        config,
        dxrLogger,
        connFinalDS,
        batchedConnDS,
        incrementalDates)
    } catch {
      case de: DXR2Exception => {
        // rethrow the error to mark the job a failure
        throw de
      }
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        dxrLogger.updateStatusLog(
          Some(config.landingDatabaseName),
          "FAILED",
          4,
          sw.toString,
          isError = true)
        // rethrow the error to mark the job as failure
        throw e
    }
  }

  def extractDataByConnections(
      config: DxrParallelDataExtractorConfig,
      dxrLogger: DXRLogger,
      connFinalDS: Dataset[DXRConnection],
      batchedConnDS: Dataset[DXRConnection],
      incrementalDates: Option[Dataset[DXRIncrementStatus]] = None)(implicit
      sparkSession: SparkSession): Dataset[DXRTaskLog] = {

    import sparkSession.implicits._

    val sc                  = sparkSession.sparkContext
    val hadoopConfiguration = sc.hadoopConfiguration

    /**
     * refer to org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat.prepareWrite.
     * these properties are necessary for parquet support writer
     */
    val configMap = Map(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> sparkSession.sessionState.conf.parquetOutputTimestampType.toString,
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> sparkSession.sessionState.conf.sessionLocalTimeZone,
      SQLConf.CASE_SENSITIVE.key -> sparkSession.sessionState.conf.caseSensitiveAnalysis.toString,
      SQLConf.PARQUET_BINARY_AS_STRING.key -> sparkSession.sessionState.conf.isParquetBinaryAsString.toString,
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key -> sparkSession.sessionState.conf.isParquetINT96AsTimestamp.toString,
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> sparkSession.sessionState.conf.writeLegacyParquetFormat.toString,
      "fs.default.name" -> (config.s3StagingBucket match {
        case Some(x) => s"s3://$x" // Use an S3 bucket when provided
        case None =>
          hadoopConfiguration.get("fs.default.name") // Use HDFS if none provided
      }))

    val noOfDbs = connFinalDS.map(_.DB_ID).distinct().count()

    if (noOfDbs == 0)
      throw DXR2Exception(
        s"No connections found in DXR Catalog for ${config.dxrProductName}")

    val schemaLogs = new ListBuffer[DXRTaskLog]()

    val applicationId = sc.applicationId
    val attemptId     = sc.applicationAttemptId

    // filter 10 connections per db_schema and sql, it should be enough to fetch schema instead of looping though entire df in case of failures
    val schemaSampleDS: Dataset[DXRConnection] = connFinalDS
      .withColumn(
        "RANK",
        row_number().over(
          Window.partitionBy("SQL_NAME", "DB_SCHEMA").orderBy("TARGET_KEY")))
      .where($"RANK" < 11)
      .drop("RANK")
      .as[DXRConnection]

    val sqlConnectionsMap: Map[String, Array[DXRConnection]] =
      schemaSampleDS.collect().groupBy(_.SQL_NAME)

    // build sqlName and schemaMap
    val schemaMap: Map[String, StructType] = sqlConnectionsMap.flatMap {
      x: (String, Array[DXRConnection]) =>
        var errorLog = null.asInstanceOf[DXRTaskLog]
        val sqlName  = x._1

        try {
          // Convert the connections list to a stream because we would want it to be lazy
          // we will care only about the first successful connection
          val schema = x._2.toStream
            .map((conn: DXRConnection) => {
              try {
                Some(dxrFunctions.getSchemaFromDXRConnection(conn, config.dxrProductName))
              } catch {
                case e: Exception =>
                  e.printStackTrace()
                  errorLog = DXRTaskLog(
                    APPLICATION_ID           = applicationId,
                    ATTEMPT_ID               = attemptId,
                    SQL_NAME                 = conn.SQL_NAME,
                    TARGET_KEY               = conn.TARGET_KEY.toInt,
                    DB_PARTITION             = conn.DB_SCHEMA,
                    FINAL_SELECT_SQL         = conn.FINAL_SQL,
                    FINAL_SESSION_SETUP_CALL = conn.FINAL_SESSION_SETUP_CALL,
                    MSG                      = "No Schema Found because of: " + e.getMessage,
                    IS_ERROR                 = true)
                  None
              }
            })
            .filter(_.isDefined)
            .head
            .get

          // make all fields nullable to avoid mismatch because of constraint sync between dbs
          val nullSafeSchema = StructType(schema.map {
            case StructField(name, dataType, _, metadata) =>
              StructField(name, dataType, nullable = true, metadata)
          })

          // Return the tuple
          Some(sqlName -> nullSafeSchema)
        } catch {
          case _: Exception =>
            schemaLogs += errorLog
            None
        }
    }

    if (schemaMap.size != sqlConnectionsMap.size) {
      dxrLogger.writeTasksLogs(schemaLogs.toDS())
      // The require statement below is redundant, but helps with program exit
      require(
        schemaMap.size == sqlConnectionsMap.size,
        "Cannot Infer Schemas for some of the input SQLs. Please revisit SQLs. For detailed logs, check the DXR catalog log tables")
    }

    val blue = config.blueDatabaseName
    val green = config.greenDatabaseName
    val landing = config.landingDatabaseName

    if (config.doMaintenanceCheck) {

      // flatten optional dbs
      val dbs = List(
        config.blueDatabaseName,
        config.greenDatabaseName).flatten :+ config.landingDatabaseName

      dxrDataWriter.checkTargetTablesForSchemaChange(
        schemaMap,
        dbs,
        config.outputPartitionSpec)
    }


    val fileSystem = getDXRFileSystem(config)

    val pathsMap: Map[String, String] = schemaMap.keys
      .map(sqlName => {
        val path =
          new Path(
            s"/tmp/${sc.sparkUser}/dxr2/$applicationId/${attemptId.getOrElse(1)}/$sqlName")
        fileSystem.mkdirs(path)
        (sqlName, path.toUri.toString)
      })
      .toMap

    val parquetBlockSize =
      hadoopConfiguration.getInt("parquet.block.size", ParquetWriter.DEFAULT_BLOCK_SIZE)
    val parquetPageSize =
      hadoopConfiguration.getInt("parquet.page.size", ParquetWriter.DEFAULT_PAGE_SIZE)

    logger.info(
      s"EXTRACT_PARALLELIZATION: DEGREE=${noOfDbs * config.maxParallelConnectionsPerDb} NO_OF_DBS=$noOfDbs CONNS_PER_DB=${config.maxParallelConnectionsPerDb}")

    schemaMap.foreach({ x =>
      {
        logger.info(x._1 + " ... " + x._2.treeString)
      }
    })

    //pre-extract table migration code starts here
    //add flag to toggle between legacy logic and new logic
    if (config.isVersioned == false) {

      //convert legacy table to delta for landingdb
      convertLegacyTableToDelta(schemaMap, config.landingDatabaseName, config)

      //convert legacy table to delta for bluedb
      try{
      if(config.skipBlueDb == false) {
        convertLegacyTableToDelta(schemaMap, config.blueDatabaseName.get, config)
      }
        }
        catch
        {
          case e: Exception => {
            logger.error(
              s"table skipped in blueDatabase",
              e)
          }
        }

      //convert legacy table to delta for greendb
      try{
      if(config.skipGreenDb == false) {
        convertLegacyTableToDelta(schemaMap, config.greenDatabaseName.get, config)
      }
        }
        catch {
          case e: Exception => {
            logger.error(
              s"table skipped in greendb",
              e)
          }
        }
    }

    val extractStartTime = System.currentTimeMillis()
    val extractLogsDS = extractDataToStaging(
      configMap,
      applicationId,
      attemptId,
      schemaMap,
      pathsMap,
      parquetBlockSize,
      parquetPageSize,
      batchedConnDS,
      config,
      Some(noOfDbs))

    logger.info(s"logging lineage for extractLogsDS in stdout")
    extractLogsDS.explain()

    val extractEndTime       = System.currentTimeMillis()
    val extractWallClockTime = extractEndTime - extractStartTime
    logger.info(s"DXR_EXTARCT_WALL_CLOCK_TIME_MS: $extractWallClockTime")

    // trigger an action on the dataframe invoke extracts
    extractLogsDS.count()
    // write logs to dxr log table
    dxrLogger.writeTasksLogs(extractLogsDS, Some(config.landingDatabaseName))

    val rowCountsMap: Map[String, Long] = extractLogsDS
      .groupBy($"SQL_NAME")
      .agg(sum($"ROW_COUNT").as("ROW_COUNT"))
      .collect()
      .map(row => {
        val sqlName: String = row.getAs("SQL_NAME")
        val rowCount: Long  = row.getAs("ROW_COUNT")
        sqlName -> rowCount
      })
      .toMap

    // If a db_schema field exists in the extract, it shall be used automatically for distribution of data before writing to hive
    val addonDistributionColumns = schemaMap.values.head.fields
      .map({ x => x.name })
      .filter({ x => x.toLowerCase.startsWith("db_schema") })
      .mkString(",")

    val executionCountsByQuery: Map[String, Long] = extractLogsDS
      .groupBy("SQL_NAME")
      .count()
      .collect()
      .map(row => {
        row.getAs[String]("SQL_NAME") -> row.getAs[Long]("count")
      })
      .toMap

    val connectionCountsByQuery: Map[String, Long] = batchedConnDS
      .groupBy("SQL_NAME")
      .count()
      .collect()
      .map(row => {
        row.getAs[String]("SQL_NAME") -> row.getAs[Long]("count")
      })
      .toMap

    val mismatchedSqls: List[String] = executionCountsByQuery.keys
      .flatMap(key => {
        val expectedQueryCount = connectionCountsByQuery(key)
        val executedQueryCount = executionCountsByQuery(key)
        if (executedQueryCount == expectedQueryCount)
          None
        else {
          logger.warn(
            s"CORRUPT_DATA_SQL: execution counts for $key was expected to be $expectedQueryCount but found $executedQueryCount")
          Some(key)
        }
      })
      .toList

    // don't write data of sql's which had mismatches in expected execution counts
    val validPathsMap     = pathsMap.filterKeys(!mismatchedSqls.contains(_))
    val validRowCountsMap = rowCountsMap.filterKeys(!mismatchedSqls.contains(_))

    val writeFailures = dxrDataWriter.writeToHiveTables(
      config       = config,
      pathsMap     = validPathsMap,
      rowCountsMap = validRowCountsMap,
      addonDistributionColumns =
        if (addonDistributionColumns != "") Some(addonDistributionColumns) else None,
      dxrLogger          = dxrLogger,
      skipStagingCleanup = config.skipStagingCleanup,
      isOverWrite        = config.isOverWrite,
      isVersioned        = config.isVersioned)

    val connectionErrorsCount = extractLogsDS.where($"IS_ERROR" === true).count
    val zeroCountSqls = rowCountsMap
      .filter(p => {
        p._2 == 0
      })
      .keys
      .toList

    if (zeroCountSqls.nonEmpty) {
      logger.warn(
        s"${zeroCountSqls.mkString(",")} SQLs have returned 0 rows across all targets. They may/may not be problem, please review.")
    }

    extractLogsDS.unpersist()
    connFinalDS.unpersist()
    val writeWallClockTime = System.currentTimeMillis() - extractEndTime
    logger.info(s"DXR_WRITE_WALL_CLOCK_TIME_MS: $writeWallClockTime")

    dxrLogger.calculateAndRecordExtractSummary(
      jobStartTime        = extractStartTime,
      taskLogsDS          = Some(extractLogsDS),
      landingDatabaseName = config.landingDatabaseName,
      corruptDataSqls     = mismatchedSqls,
      failedWrites        = writeFailures)

    processExtractResult(
      incrementalDates      = incrementalDates,
      dxrLogger             = dxrLogger,
      strictMode            = config.strictMode,
      landingDatabaseName   = config.landingDatabaseName,
      corruptDataSqls       = mismatchedSqls,
      zeroCountSqls         = zeroCountSqls,
      failedWrites          = writeFailures,
      connectionErrorsCount = connectionErrorsCount,
      extractWallClockTime  = extractWallClockTime,
      writeWallClockTime    = writeWallClockTime)

    extractLogsDS
  }

  /**
   * checks if a table exists
   *
   * @param table
   */
  def tableExists(table: String)(implicit spark: SparkSession) =
    spark.catalog.tableExists(table)


  /**
   * convert legacy tables from parquet format to delta format
   *
   * @param schemaMap
   * @param databaseName
   * @param config
   */
  def convertLegacyTableToDelta(
      schemaMap: Map[String, StructType],
      databaseName: String,
      config: DxrParallelDataExtractorConfig)(implicit sparkSession: SparkSession):  Unit = {

    schemaMap.foreach(tbl => {
      val tbl_name = tbl._1
      val tbl_schema = tbl._2

      val formatEncoder = Encoders.STRING
      val targetTableName = s"$databaseName.$tbl_name"
      if (tableExists(targetTableName) == true) {
      //check if table is not delta format and is external or managed (if managed convert to external)

        if (sparkSession.sql(s"DESC DETAIL $targetTableName")
          .select("format")
          .as(formatEncoder)
          .head != "delta") {
          if (sparkSession.sql(s"desc formatted $targetTableName")
            .filter(col("col_name") === "Type")
            .select("data_type")
            .as(formatEncoder)
            .head == "MANAGED") {
            //convert the managed table to external table
            logger.info(
              s"converting managed table $targetTableName to external")
            try {
              hiveTableUtils.convertTableType(targetTableName, CatalogTableType.EXTERNAL)
            } catch {
              case e: Exception => {
                logger.error(
                  s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to convert managed table $targetTableName to external",
                  e)
                throw e
              }
            }
          }

          val location = sparkSession.sql(s"desc formatted $targetTableName")
            .filter(col("col_name") === "Location")
            .select("data_type")
            .as(formatEncoder)
            .head

          val pattern = "_[v|V]\\d+".r
          val latest_ver = pattern.findFirstIn(location) match {
            case Some(matched) => location.substring(location.indexOf(matched))
            case None => ""
          }

          if (latest_ver == ""){
            //convert table to delta in place
            sparkSession.sql(s"CONVERT TO DELTA $targetTableName")
            //tbl1 --> tbl1_v1
            //tbl2 --> tbl2
            //sorted but not versioned ---> delta ---> check column sorting during insert

            // Mark other versions of the table as to be dropped
            sparkSession.sql(s"""create table if not exists $databaseName.dxr_converted_table_info (schemaName String, tableName String, location String, version String) using delta""")
            sparkSession.sql(s"""insert into $databaseName.dxr_drop_table_info values('$databaseName', '$tbl_name', '$location', '$latest_ver')""")

            //generate manifest file
            try {
              val deltaTable = DeltaTable.forPath(location)
              deltaTable.generate("symlink_format_manifest")
            } catch {
              case e: Exception => {
                logger.error(
                  s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to create manifest files for $targetTableName",
                  e)
                throw e
              }
            }
          }
          else {
            var new_table_path = location.replaceAll(latest_ver, "")

            val versionedTableName = s"$targetTableName$latest_ver"

            //drop the external table
            logger.info(
              s"dropping external legacy table $targetTableName")
            sparkSession.sql(s"drop table $targetTableName")

            //create empty delta table using new schema
            logger.info(
              s"creating new delta table $targetTableName")
            val emptyDF: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], tbl_schema)

            if (databaseName.contains("blue_raw") || databaseName.contains("blue_landing")) {
              println("no hashing")
              logger.info(
                s"creating new delta table $targetTableName")
              try {
                DeltaDataFrameWriter.insert(
                  tableName = targetTableName,
                  inputDf = emptyDF,
                  isOverWrite = false,
                  partitionSpec = config.outputPartitionSpec)
              } catch {
                case e: Exception => {
                  logger.error(
                    s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to create new delta table $targetTableName",
                    e)
                  throw e
                }
              }
            } else {
              val columnsForHashing = (emptyDF.columns.filter { x =>
                x.endsWith("_hash")
              } ++ config.extraHashColumnsList).distinct
              val hashedDf = columnsForHashing.foldLeft(emptyDF)({ (df, column) =>
                df.withColumn(column, hashUdf(col(column)))
              })
              try {
                DeltaDataFrameWriter.insert(
                  tableName = targetTableName,
                  inputDf = hashedDf,
                  isOverWrite = false,
                  partitionSpec = config.outputPartitionSpec)
              } catch {
                case e: Exception => {
                  logger.error(
                    s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to create new delta table $targetTableName",
                    e)
                  throw e
                }
              }
            }



//            logger.info(
//              s"creating new delta table $targetTableName")
//            try {
//              DeltaDataFrameWriter.insert(
//                tableName = targetTableName,
//                inputDf = hashedDf,
//                isOverWrite = false,
//                partitionSpec = config.outputPartitionSpec)
//            } catch {
//              case e: Exception => {
//                logger.error(
//                  s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to create new delta table $targetTableName",
//                  e)
//                throw e
//              }
//            }

            //Msck repair on the versioned table
            try {
              sparkSession.sql(s"""MSCK REPAIR TABLE $versionedTableName""")
            } catch {
              case e: Exception => {
                logger.error(
                  s"MSCK REPAIR TABLE failed on $targetTableName",
                  e)
              }
            }

            //Insert data from versioned table into new delta table
            try {
              insertDataFromVersionedTableToNewTable(targetTableName, versionedTableName)
            } catch {
              case e: Exception => {
                logger.error(
                  s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to copy data from versioned table to new delta $targetTableName",
                  e)
                throw e
              }
            }


            // Mark other versions of the table as to be dropped
            sparkSession.sql(s"""create table if not exists $databaseName.dxr_converted_table_info (schemaName String, tableName String, location String, version String) using delta""")
            sparkSession.sql(s"""insert into $databaseName.dxr_drop_table_info values('$databaseName', '$tbl_name', '$location', '$latest_ver')""")

            //generate manifest file
            try {
              val deltaTable = DeltaTable.forPath(new_table_path)
              deltaTable.generate("symlink_format_manifest")
            } catch {
              case e: Exception => {
                logger.error(
                  s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to create manifest files for $targetTableName",
                  e)
                throw e
              }
            }
          }
        }
      }
      else {
        println("new table")
      }
    })
  }


  /**
   * copies data from versioned table to new delta table
   *
   * @param targetTableName
   * @param versionedTableName
   */
  def insertDataFromVersionedTableToNewTable(
      targetTableName: String,
      versionedTableName: String)(implicit sparkSession: SparkSession): Unit = {

    val columnList_new = sparkSession.sql(s"""select * from $targetTableName limit 0""").columns
    val columnList2_ver = sparkSession.sql(s"""select * from $versionedTableName limit 0""").columns

    var insertString = s"insert into $targetTableName select "

    if ((columnList_new.diff(columnList2_ver).nonEmpty && columnList2_ver.diff(columnList_new).isEmpty) || (columnList_new.diff(columnList2_ver).nonEmpty && columnList2_ver.diff(columnList_new).nonEmpty)) {
      val newCols = columnList_new.diff(columnList2_ver)
      columnList2_ver.foreach { element =>
        insertString += s"$element,"
      }
      newCols.zipWithIndex.foreach { case (element, index) =>
        if (index == newCols.length - 1) {
          insertString += s"null as $element"
        } else {
          insertString += s"null as $element,"
        }
      }
    } else {
      columnList_new.zipWithIndex.foreach { case (element, index) =>
        if (index == columnList_new.length - 1) {
          insertString += s"$element"
        } else {
          insertString += s"$element,"
        }
      }
    }

    insertString += s" from $versionedTableName"
    logger.info(
      s"copying data from $versionedTableName to $targetTableName")
    try {
      sparkSession.sql(insertString)
    } catch {
      case e: Exception => {
        logger.error(
          s"LEGACY_TABLE_CONVERSION_FAILURE:failure while trying to copy data from $versionedTableName to $targetTableName",
          e)
        throw e
      }
    }
  }

  /**
   * extracts data to staging directory and returns a data frame without lineage to avoid re-invocation of the extract due to any down stream actions on the dataframe
   *
   * scale down or loss of nodes causes spark to recompute shuffle files,
   * this triggers a re run on some of the sqls and leads to data corruption.
   *
   * @see [[https://jira.service.tools-pi.com/browse/DCAN-10313]] for complete details
   * @param configMap
   * @param applicationId
   * @param attemptId
   * @param schemaMap
   * @param pathsMap
   * @param parquetBlockSize
   * @param parquetPageSize
   * @param batchedConnDS
   * @param config
   * @param sparkSession
   * @return
   */
  def extractDataToStaging(
      configMap: Map[String, String],
      applicationId: String,
      attemptId: Option[String],
      schemaMap: Map[String, StructType],
      pathsMap: Map[String, String],
      parquetBlockSize: Int,
      parquetPageSize: Int,
      batchedConnDS: Dataset[DXRConnection],
      config: DxrParallelDataExtractorConfig,
      dbsCount: Option[Long])(implicit sparkSession: SparkSession) = {

    // TODO: instead of retry immediately on failure try to retry after all the connections are done.

    import sparkSession.implicits._

    // repartition by max connections per DB
    val distributedConnectionsDS: Dataset[DXRConnection] =
      distributeConnectionsByDBLimit(config, batchedConnDS, dbsCount)

    logger.info(
      "Total number of Queries being fired = " + distributedConnectionsDS.count())

    val extractReportDS: Dataset[DXRTaskLog] = distributedConnectionsDS
      .mapPartitions(partition => {
        if (partition.hasNext) {

          // All the below logic will be executed with "ONE TASK"

          val dxrConnections: List[DXRConnection] = partition.toList

          val connectionsBySqlMap = dxrConnections.groupBy(_.SQL_NAME)

          val taskContext = org.apache.spark.TaskContext.get

          val dxrTaskLogs = connectionsBySqlMap.flatMap(x => {
            // create and close a writer against each sql
            val sqlName = x._1
            // File path is unique for a given task.
            val stagingPath = pathsMap(sqlName)
            val filePath =
              s"$stagingPath/dxr_part_${taskContext.taskAttemptId}.parquet"
            val schema   = schemaMap(sqlName)
            val dxrConns = x._2

            val parquetWriter = dxrParquetFunctions.getParquetWriterInstance(
              configMap,
              filePath,
              schema,
              parquetBlockSize,
              parquetPageSize,
              config.compressionCodec)

            val logs = dxrConns.map(dxrConn => {
              dxrFunctions.writeToHDFS(
                dxrConn                   = dxrConn,
                dxrProductName            = config.dxrProductName,
                parquetCatalystWriter     = parquetWriter,
                filePath                  = stagingPath,
                jdbcSchema                = schema,
                applicationId             = applicationId,
                attemptId                 = attemptId,
                dxrLoggingConnectionProps = config.dxrConnectionProperties,
                dxrLoggingJdbcUrl         = config.dxrJdbcUrl,
                fetchSize                 = config.fetchArraySize,
                maxRows                   = config.limit,
                maxTimeOut                = config.queryTimeout,
                retryIntervalSecs         = config.retryIntervalSecs)
            })

            try {
              logger.info("Closing Writer for path = " + filePath)
              parquetWriter.close()
            } catch {
              case _: Exception =>
                logger
                  .info("Unable to close parquetwriter for path " + filePath)
            }
            logs.iterator
          })
          dxrTaskLogs.iterator
        } else {
          Iterator.empty
        }
      })

    logger.info(
      s"DXR_EXTRACT_START: triggering the extract by removing lineage from extractReportDS")

    extractReportDS.collect().toList.toDS()
  }

  /**
   * get a handle for the filesystem where temporary data shall be written
   *
   * @param config
   * @param hadoopConfiguration
   * @return
   */
  def getDXRFileSystem(config: DxrParallelDataExtractorConfig)(implicit
      sparkSession: SparkSession): FileSystem = {
    val fs = FileSystem.get(
      config.s3StagingBucket match {
        case Some(x) => {
          val s3path = s"s3://$x"
          new URI(s3path)
        } // Use an S3 bucket when provided
        case None =>
          new URI(
            sparkSession.sparkContext.hadoopConfiguration.get("fs.default.name")
          ) // Use HDFS if none provided
      },
      sparkSession.sparkContext.hadoopConfiguration)
    fs
  }

  /**
   * redistributes the dataframe connections so that a connections to a given db do not exceed the max connections allowed.
   * this is achieved by controlling the of dataframe partitions and the dxr connections in each partition
   *
   * @param config
   * @param batchedConnDS
   * @param dbCount
   * @param sparkSession
   * @return
   */
  def distributeConnectionsByDBLimit(
      config: DxrParallelDataExtractorConfig,
      batchedConnDS: Dataset[DXRConnection],
      dbCount: Option[Long])(implicit
      sparkSession: SparkSession): Dataset[DXRConnection] = {

    import sparkSession.implicits._

    val noOfDbs: Int = dbCount match {
      case Some(x) => x.toInt
      case _       => batchedConnDS.select("DB_ID").distinct().count().toInt
    }

    val maxPartitions = noOfDbs * config.maxParallelConnectionsPerDb

    val repartitionedDf = batchedConnDS
      .withColumn(
        "PARTITION_KEY",
        $"DB_ID" * config.maxParallelConnectionsPerDb +
          $"ROW_ID" % config.maxParallelConnectionsPerDb)
      .as[DXRConnection]

    repartitionedDf.repartitionByRange(maxPartitions, $"PARTITION_KEY")
  }

  def getbatchedSelectSqls(
      batchColumnName: String,
      finalSql: String,
      numPartitions: Int = 0,
      upperBound: Long   = 0,
      lowerBound: Long   = 0) = {
    // original code ref: org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation.columnPartition(partitioning: JDBCPartitioningInfo)

    val batchedSqls = numPartitions match {
      case 0 => List((finalSql, 0))
      case _ =>
        // Overflow and silliness can happen if you subtract then divide.
        // Here we get a little roundoff, but that's (hopefully) OK.
        val stride = (upperBound / numPartitions
          - lowerBound / numPartitions)
        var i: Int             = 0
        var currentValue: Long = lowerBound
        var ansTuple           = new ArrayBuffer[(String, Int)]()
        while (i < numPartitions) {
          val lowerBound =
            if (i != 0) s"$batchColumnName >= $currentValue" else null
          currentValue += stride
          val upperBound =
            if (i != numPartitions - 1) s"$batchColumnName < $currentValue"
            else null
          val whereClause =
            if (upperBound == null) {
              lowerBound
            } else if (lowerBound == null) {
              upperBound
            } else {
              s"$lowerBound AND $upperBound"
            }

          val batchedSql =
            s"SELECT * FROM (\n $finalSql \n) alias_does_matter WHERE $whereClause"
          val tuple = (batchedSql, i)
          ansTuple += tuple
          i = i + 1
        }
        ansTuple.toList
    }
    batchedSqls
  }

  def getBatchedConnectionsDF(
      batchColumnName: String,
      numBatches: Int,
      connectionsDS: Dataset[DXRConnection])(implicit
      sparkSession: SparkSession): Dataset[DXRConnection] = {

    import sparkSession.implicits._

    val batchSelectSqlsUdf = udf(
      (
          batchColumnName: String,
          finalSql: String,
          numPartitions: Int,
          upperBound: Long,
          lowerBound: Long) => {
        getbatchedSelectSqls(
          batchColumnName,
          finalSql,
          numPartitions,
          upperBound,
          lowerBound)
      })

    val batchedDS = if (connectionsDS.select($"BATCH_NUMBER").distinct().count() > 1) {
      logger.info("using batching provided in catalog query")
      connectionsDS
        .withColumn(
          "FINAL_SQL",
          concat(
            lit("SELECT * FROM (\n"),
            $"FINAL_SQL",
            lit(s" \n) alias_does_matter WHERE $batchColumnName >="),
            $"LOWER_BOUND",
            lit(s" AND $batchColumnName <= "),
            $"UPPER_BOUND"))
        .as[DXRConnection]
    } else {
      connectionsDS
        .withColumn(
          "FINAL_SQL",
          explode(
            batchSelectSqlsUdf(
              lit(batchColumnName),
              $"FINAL_SQL",
              lit(numBatches),
              $"UPPER_BOUND",
              $"LOWER_BOUND")))
        .withColumn("BATCH_NUMBER", $"FINAL_SQL._2")
        .withColumn("FINAL_SQL", $"FINAL_SQL._1")
        .withColumn("ROW_ID", monotonically_increasing_id)
        .as[DXRConnection]

    }
    batchedDS
      .withColumn(
        "FINAL_SQL",
        regexp_replace($"FINAL_SQL", lit("\\[BATCHNUMBER\\]"), $"BATCH_NUMBER"))
      .as[DXRConnection]
  }

  def getConnectionsDfFromConfig(
      config: DxrParallelDataExtractorConfig,
      incrementalDates: Option[Dataset[DXRIncrementStatus]] = None)(implicit
      sparkSession: SparkSession): Dataset[DXRConnection] = {

    val isolationLevel = config.isolationLevel
    val sqlFileNames   = config.sqlFileNames
    val sqlStringsMap  = config.sqlStringsMap
    val upperBound     = config.upperBound
    val lowerBound     = config.lowerBound

    getConnectionsDfFromDxrBaseConfig(
      config,
      incrementalDates,
      sqlFileNames,
      sqlStringsMap,
      isolationLevel,
      upperBound,
      lowerBound,
      dxrSql = config.dxrSql)
  }

}
