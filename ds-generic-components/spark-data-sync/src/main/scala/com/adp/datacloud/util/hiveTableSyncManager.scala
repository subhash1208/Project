package com.adp.datacloud.util

import java.io.{PrintWriter, StringWriter}
import java.util.{Calendar, Date, Properties}
import com.adp.datacloud.cli.{HiveTableSyncManagerConfig, hiveTableSyncManagerOptions}
import com.adp.datacloud.writers.{HiveDataFrameWriter, DataCloudDataFrameWriterLog}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.CannedAccessControlList
import oracle.jdbc.OracleConnection
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParSeq

case class SyncUnit(
  SOURCE_TABLE:           String,
  SOURCE_SCHEMA:          String,
  TARGET_SCHEMA:          String,
  FORCE_SYNC_DAY_OF_MNTH: Int     = 0,
  ACTIVE:                 Boolean = true,
  MAX_HIST_PARTITIONS:    Int     = 0,
  IS_VIEW:                Boolean = false)

object hiveTableSyncManager {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {
    
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")
     
    val config = hiveTableSyncManagerOptions.parse(args)

    implicit val sparkSession: SparkSession =
      if (System
        .getProperty("os.name")
        .toLowerCase()
        .contains("windows") || System
        .getProperty("os.name")
        .toLowerCase()
        .startsWith("mac")) {
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

    val failedTableNames = buildAndRunSync(config)

    if (failedTableNames.nonEmpty) {
      throw new Exception(
        s"some of table failed to sync FAILED_TABLES_WRITES=$failedTableNames")
    }
  }

  def buildAndRunSync(config: HiveTableSyncManagerConfig)(
    implicit
    sparkSession: SparkSession): String = {
    val catalogConnectionProperties = new Properties()
    catalogConnectionProperties.setProperty(
      "driver",
      "oracle.jdbc.driver.OracleDriver")

    if (config.catalogDatabaseUser.isDefined && config.catalogDatabasePassword.isDefined) {
      catalogConnectionProperties.put("user", s"${config.catalogDatabaseUser.get}")
      catalogConnectionProperties.put(
        "password",
        s"${config.catalogDatabasePassword.get}")
    } else {
      // pass the the jdbc url including user name and password if wallet is not setup for local mode
      // jdbc:oracle:thin:<UserName>/<password>@<hostname>:<port>/<serviceID>
      catalogConnectionProperties.setProperty(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION, config.oracleWalletLocation)
    }

    logger.info("CATALOG_CONNECTION_STRING= " + {
      config.catalogJdbcUrl
    })

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    import sparkSession.implicits._

    logger.debug(write(sparkSession.conf.getAll))

    val logs = if (config.syncSchemasMap.isDefined) {
      syncTablesBySchemas(config, catalogConnectionProperties)
    } else {
      syncTablesByCalatog(config, catalogConnectionProperties)
    }

    if (logs.isDefined) {
      val logsDS: Dataset[DataCloudDataFrameWriterLog] = logs.get
      try {
        logsDS.write
          .mode(SaveMode.Append)
          .jdbc(
            config.catalogJdbcUrl,
            "SPARK_HIVE_WRITER_JOB_LOGS",
            catalogConnectionProperties)
      } catch {
        case e: Exception =>
          logger.error("Error while writing dxr hiveWriter log", e)
          e.printStackTrace()
      }

      val failedWrites = logsDS
        .where($"IS_ERROR" === true)
        .collect()
        .map(_.TABLE_NAME)

      failedWrites.mkString(",")

    } else {
      s""
    }

  }

  def syncTablesByCalatog(config: HiveTableSyncManagerConfig, catalogConnectionProperties: Properties)(
    implicit
    sparkSession: SparkSession): Option[Dataset[DataCloudDataFrameWriterLog]] = {
    import sparkSession.implicits._

    logger.info("CATALOG_SQL_STRING = " + config.catalogSql(config.tableName))

    val catalogRowsList: List[SyncUnit] = sparkSession.read
      .jdbc(config.catalogJdbcUrl, config.catalogSql(config.tableName), catalogConnectionProperties)
      .as[SyncUnit]
      .collect()
      .toList

    val catalogRowsFullList: List[SyncUnit] = sparkSession.read
      .jdbc(config.catalogJdbcUrl, config.catalogSql(), catalogConnectionProperties)
      .as[SyncUnit]
      .collect()
      .toList

    val logs = if (config.ddlOnly) {
      None
    } else {
      val logsDS =
        syncTables(
          catalogRowsList,
          config.adpInternalClients,
          config.parallelHiveWrites).toList.toDS
      logsDS.show(false)
      Some(logsDS)
    }

    extractDDLs(catalogRowsFullList, config.s3bucketName, config.ddlFileName, config.ddlReplaceMap)
    logs
  }

  def syncTablesBySchemas(config: HiveTableSyncManagerConfig, catalogConnectionProperties: Properties)(
    implicit
    sparkSession: SparkSession): Option[Dataset[DataCloudDataFrameWriterLog]] = {
    import sparkSession.implicits._

    val schemaMap = config.syncSchemasMap.get

    val schemaTablesMap: Map[String, List[SyncUnit]] = schemaMap.map(schemaPair => {
      val sourceSchema = schemaPair._1
      val targetSchema = schemaPair._2
      val catalogRowsList = sparkSession.catalog.listTables(sourceSchema).map(table => SyncUnit(table.name, table.database, targetSchema)).as[SyncUnit].collect().toList
      (sourceSchema -> catalogRowsList)
    })

    val fullTablesList: List[SyncUnit] = schemaTablesMap.flatMap(_._2).toList

    val logs = if (config.ddlOnly) {
      None
    } else {
      val logsDS: Dataset[DataCloudDataFrameWriterLog] = schemaTablesMap.flatMap(tableMap => {
        val tablesList = tableMap._2
        logger.info(s"started syncing schema ${tableMap._1}")
        val logs = syncTables(
          tablesList,
          config.adpInternalClients,
          config.parallelHiveWrites)
        logger.info(s"finished syncing schema ${tableMap._1}")
        logs.toList
      }).toList.toDS()
      logsDS.show()
      Some(logsDS)
    }

    extractDDLs(fullTablesList, config.s3bucketName, config.ddlFileName, config.ddlReplaceMap)
    logs
  }

  def syncTables(
    catalogRowsList:    List[SyncUnit],
    adpInternalClients: String,
    parallelHiveWrites: Int            = 1)(
    implicit
    sparkSession: SparkSession): ParSeq[DataCloudDataFrameWriterLog] = {
    val catalog = sparkSession.catalog
    val sc = sparkSession.sparkContext
    val cal = Calendar.getInstance()
    cal.setTime(new Date)
    val currentDayOfMonth = cal.get(Calendar.DAY_OF_MONTH)
    val catalogTablesListParalleled = catalogRowsList.par
    catalogTablesListParalleled.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(parallelHiveWrites))

    catalogTablesListParalleled.flatMap(syncUnit => {
      val sourceTable = syncUnit.SOURCE_TABLE
      val sourceSchema = syncUnit.SOURCE_SCHEMA
      val targetSchema = syncUnit.TARGET_SCHEMA
      val forceSyncDay = syncUnit.FORCE_SYNC_DAY_OF_MNTH

      val forceSync = if (forceSyncDay > 0 && currentDayOfMonth == forceSyncDay) {
        true
      } else {
        false
      }

      val sourceTableName = s"$sourceSchema.$sourceTable"
      val targetTableName = s"$targetSchema.$sourceTable"

      try {
        val sourceColumns = catalog.listColumns(sourceSchema, sourceTable)
        val partitionColumns = sourceColumns
          .filter(col("isPartition") === true)
          .collect()
          .map(_.name.toLowerCase)
        val orderedColumns = sourceColumns
          .filter(col("isPartition") === false)
          .collect()
          .map(_.name.toLowerCase) ++ partitionColumns

        val partitionsColumnsString = partitionColumns.mkString(",")

        val sourceTableSchema = sparkSession
          .table(sourceTableName).schema

        // remove adp internal clients
        val filteredSql = if (orderedColumns.contains("clnt_obj_id")) {
          s"select * from $sourceTableName where clnt_obj_id not in ('$adpInternalClients') or trim(clnt_obj_id) is null"
        } else if (orderedColumns.contains("ooid")) {
          s"select * from $sourceTableName where ooid not in ('$adpInternalClients') or trim(ooid) is null"
        } else {
          s"select * from $sourceTableName"
        }

        val syncStatus = if (catalog.tableExists(targetSchema, sourceTable)) {
          val dropTableSql = s"DROP TABLE IF EXISTS $targetTableName"
          val targetPartitionColumnsString = catalog
            .listColumns(targetSchema, sourceTable)
            .filter(col("isPartition") === true)
            .collect()
            .map(_.name.toLowerCase)
            .mkString(",")

          val sourceFieldSet =
            getFiledSet(sourceTableSchema).sorted.mkString(",")
          val targetFieldSet =
            getFiledSet(sparkSession.table(targetTableName).schema).sorted
              .mkString(",")

          val isTableEmpty = if (partitionColumns.isEmpty) {
            sparkSession.table(targetTableName).isEmpty
          } else {
            // doing show partitions is easier but on empty table it throws errors
            sparkSession.sessionState.catalog.listPartitionNames(TableIdentifier(sourceTable, Some(targetSchema)), None).isEmpty
          }

          // verify schema match comparing all columns with datatypes along with partition columns names
          val schemaMismatch =
            !((sourceFieldSet == targetFieldSet) && (partitionsColumnsString == targetPartitionColumnsString))

          if (forceSync || isTableEmpty || schemaMismatch) {

            if (forceSync) logger.warn(s"DROP_FORCE_SYNC: $targetTableName")
            if (isTableEmpty) logger.warn(s"DROP_EMPTY: $targetTableName")
            if (schemaMismatch) logger.warn(s"DROP_SCHEMA_MISMATCH: $targetTableName")
            /*
             * Incases where: configured for forced sync/ target table is Empty / schema doesn't match
             * drop the target table to avoid creation of a bkp by hive dataframe writer or any overhead
        	   */
            logger.info(dropTableSql)
            sparkSession.sql(dropTableSql)
            Some(filteredSql)
          } else {
            val syncDf: Option[String] =
              if (partitionColumns.nonEmpty) {

                val sourcePartitions = sparkSession.sessionState.catalog
                  .listPartitionNames(TableIdentifier(sourceTable, Some(sourceSchema)), None)
                val destinationPartitionsLowerCase = sparkSession.sessionState.catalog
                  .listPartitionNames(TableIdentifier(sourceTable, Some(targetSchema)), None).map(_.toLowerCase())

                val missingPartitions = sourcePartitions
                  .filterNot(x => {
                    // convert to lower case here for comparison but make sure the final sql contains the actual case for proper sql execution
                    destinationPartitionsLowerCase.contains(x.toLowerCase)
                  })

                if (missingPartitions.isEmpty) {
                  // mark the table in sync if all the partitions match
                  logger.warn(s"SKIP_SYNC: all partitions match for $targetTableName")
                  None
                } else {
                  val missingPartitionsSql = missingPartitions
                    .map(
                      _.replaceAll("/", "' AND ")
                        .replaceAll("=", "='")
                        .trim() + "'")
                    .mkString("(", ")\n OR (", ")")
                    .trim
                  val syncSql =
                    s"select * from ($filteredSql)\n where $missingPartitionsSql"
                  Some(syncSql)
                }
              } else {
                //TODO: identify change when the table has no partitions
                // if the table has no partitions so nothing.
                logger.warn(s"SKIP_SYNC: unpartitioned is not empty $targetTableName")
                None
              }
            syncDf
          }

        } else {
          // simply mark table out of sync to copy the table completely
          Some(filteredSql)
        }

        if (syncStatus.isEmpty) {
          // do nothing if the table is already in sync
          Some(
            DataCloudDataFrameWriterLog(
              sc.applicationId,
              sc.applicationAttemptId,
              targetTableName,
              0,
              0,
              0,
              0,
              s"Table $targetTableName already in sync with $sourceTableName"))

        } else {
          logger.info(s"SYNC_SQL: ${syncStatus.get}")
          println(s"SYNC_SQL: ${syncStatus.get}")

          val outputPartitionSpec =
            if (partitionColumns.isEmpty) None
            else Some(partitionsColumnsString)

          val writer = HiveDataFrameWriter("parquet", outputPartitionSpec, None)

          val syncDf = sparkSession.sql(syncStatus.get)

          // create table explicitly skipping column sort
          writer.createTableIfNotExists(
            targetTableName,
            syncDf.schema,
            skipColumnSort = true)

          // write the table that's filtered of ADP internal client and partitions in sync
          val writelog = writer.insert(targetTableName, syncDf)
          println(writelog)
          Some(writelog)
        }

      } catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error(
            s"Failure while writing to table $targetTableName from $sourceTableName",
            e)
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          Some(
            DataCloudDataFrameWriterLog(
              sparkSession.sparkContext.applicationId,
              sparkSession.sparkContext.applicationAttemptId,
              targetTableName,
              0,
              0,
              0,
              0,
              sw.toString,
              IS_ERROR = true))
      }

    })
  }

  def extractDDLs(
    catalogRowsList: List[SyncUnit],
    s3buketName:     String,
    ddlFileName:     String, stringReplacementsMap: Map[String, String])(implicit sparkSession: SparkSession) {
    val validPropertiesList = List("parquet.compression")
    val ddls = catalogRowsList
      .flatMap(syncUnit => {
        val tableName = syncUnit.TARGET_SCHEMA + "." + syncUnit.SOURCE_TABLE
        try {
          val props = sparkSession
            .sql(s"SHOW TBLPROPERTIES $tableName")
            .collect()
            .map(row => row.getAs[String]("key") -> row.getAs[String]("value"))
            .toMap
          // filter properties that are needed and ignore the rest like createTime,createdBy etc..
          val cleanedProps = props
            .filter(x => validPropertiesList.contains(x._1))
            .map(x => s"'${x._1}'  = '${x._2}'")
            .toList
          // discard existing table properties and replace them with filtered properties
          val createTableStmt = sparkSession
            .sql(s"SHOW CREATE TABLE $tableName")
            .collect()
            .map(_.getAs[String]("createtab_stmt"))
            .mkString
            .replace("CREATE TABLE", "CREATE EXTERNAL TABLE IF NOT EXISTS")
            .split("TBLPROPERTIES")(0)
          val tableProperties = if (cleanedProps.isEmpty) {
            s";"
          } else {
            cleanedProps.mkString("TBLPROPERTIES(\n", ",\n", "\n);")
          }
          val repairTable = if (createTableStmt.contains("PARTITIONED BY")) {
            s"MSCK REPAIR TABLE $tableName;"
          } else {
            s""
          }
          val dropTableStmt = s"DROP TABLE IF EXISTS $tableName;"

          val ddlString =
            Seq(dropTableStmt, createTableStmt, tableProperties, repairTable)
              .mkString(s"-------$tableName-----\n", "\n", "\n")
          logger.info(s"DDL for $tableName: $ddlString")
          Some(ddlString)
        } catch {
          case e: Exception => {
            logger.error(s"error generating ddl for $tableName", e)
            None
          }

        }
      })
      .mkString("\n")

    val processedDDls = stringReplacementsMap.foldLeft(ddls)((ddl, p) => {
      ddl.replaceAll(p._1, p._2)
    })

    val s3Client =
      AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build
    s3Client.putObject(s3buketName, ddlFileName, processedDDls)
    s3Client.setObjectAcl(
      s3buketName,
      ddlFileName,
      CannedAccessControlList.BucketOwnerFullControl)
  }

  def getFiledSet(hiveSchema: StructType): Array[String] = {
    hiveSchema.fields.map(x => {
      x.name.toLowerCase + (x.dataType.simpleString match {
        case "string" => "string"
        case _        => "nonstring"
      })
    })
  }
}