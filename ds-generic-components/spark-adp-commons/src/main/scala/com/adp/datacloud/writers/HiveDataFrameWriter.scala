package com.adp.datacloud.writers

import com.adp.datacloud.ds.hive.hiveTableUtils
import com.adp.datacloud.writers.HiveDataFrameWriter.{
  checkLatestTableAgainstBackup,
  cleanUpOldTableVersions,
  getVersionedTableNamesMap
}
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec

case class DataCloudDataFrameWriterLog(
    APPLICATION_ID: String,
    ATTEMPT_ID: Option[String],
    TABLE_NAME: String,
    DF_APPROX_SIZE_MB: Integer,
    TABLE_PARTITIONS: Integer,
    DF_PARTITIONS: Integer,
    WRITE_TIME_SECS: Integer,
    MSG: String       = null,
    IS_ERROR: Boolean = false)

/**
 * Abstract implementation of DataFrame Writers to hive tables
 */
abstract case class HiveDataFrameWriter private[writers] (
    partitionsString: String,
    partitionColumns: List[String],
    staticPartitionMap: Map[String, String],
    addonDistributionColumns: List[String])(implicit sparkSession: SparkSession) {

  private val logger = Logger.getLogger(getClass())

  val hiveDataFrameWriterVersion: String = getClass.getPackage.getImplementationVersion

  val shufflePartitons: Int =
    sparkSession.conf.get("spark.sql.shuffle.partitions", "200").toInt

  /**
   * Converts a dataframe schema into list of field definitions for hive. For eg: "clnt_obj_id:StringType" gets converted to "clnt_obj_id string"
   */

  def getColumnDefinitions(schema: StructType): Array[(String, String)]

  def sparkFormatSpecifier: String

  def sparkTableProperties: String = ""

  def hiveFormatSpecifier: String

  def hiveTableProperties: String = ""

  lazy val getStaticPartitionCols: List[String] = staticPartitionMap.keys.toList

  lazy val dynamicPartitionColumns: List[String] =
    partitionColumns.filterNot(staticPartitionMap.contains(_))

  /**
   * returns the corresponding hive data type for an input org.apache.spark.sql.types.DataType
   *
   * @see org.apache.spark.sql.hive.HiveMetastoreTypes#toMetastoreType
   */
  def toHiveMetastoreType(dt: DataType): String =
    dt match {
      case ArrayType(elementType, _) => s"array<${toHiveMetastoreType(elementType)}>"
      case StructType(fields) =>
        s"struct<${fields.map(f => s"${f.name}:${toHiveMetastoreType(f.dataType)}").mkString(",")}>"
      case MapType(keyType, valueType, _) =>
        s"map<${toHiveMetastoreType(keyType)},${toHiveMetastoreType(valueType)}>"
      case StringType    => "string"
      case FloatType     => "float"
      case IntegerType   => "int"
      case ByteType      => "tinyint"
      case ShortType     => "smallint"
      case DoubleType    => "double"
      case LongType      => "int"
      case BinaryType    => "binary"
      case BooleanType   => "boolean"
      case DateType      => "date"
      case TimestampType => "timestamp"
      //    case udt: UserDefinedType[_]        => toMetastoreType(udt.sqlType)
      case x => if (x.typeName.startsWith("decimal")) "double" else "string"
      // Override decimals with doubles for now
      // DecimalType is not tending itself to pattern matching hence the if-else
    }

  def createTableIfNotExists(
      tableName: String,
      schema: StructType,
      location: Option[java.net.URI] = None,
      skipColumnSort: Boolean        = false): StructType = {
    if (sparkSession.catalog.tableExists(tableName)) {
      sparkSession.table(tableName).schema
    } else {
      createTable(tableName, schema, location, skipColumnSort)
    }
  }

  def createTable(
      tableName: String,
      schema: StructType,
      location: Option[java.net.URI] = None,
      skipColumnSort: Boolean        = false): StructType = {
    val fields = if (skipColumnSort) {
      getColumnDefinitions(schema)
    } else {
      getColumnDefinitions(schema) sortBy (_._1)
    }

    val locationString = if (location.isDefined) {
      val locationUri = location.get
      // check if path refers to local file system
      if (locationUri.getScheme == "file") {
        Some(locationUri.getPath)
      } else {
        Some(locationUri.toString())
      }
    } else None

    val ddl = getCreateTableDDL(tableName, fields, locationString)
    logger.info(ddl)
    sparkSession.sql(ddl)
    val setPropertySql =
      s"ALTER TABLE $tableName SET TBLPROPERTIES ('hivedataframewriter.tablecreate.ver'='$hiveDataFrameWriterVersion')"
    logger.info(setPropertySql)
    sparkSession.sql(setPropertySql)

    // Return the schema of the table
    sparkSession.sql("select * from " + tableName + " where 1 = 0").schema

  }

  def getCreateTableDDL(
      tableName: String,
      fields: Array[(String, String)],
      location: Option[String]): String

  def getCreateSparkTableDDL(
      tableName: String,
      fields: Array[(String, String)],
      location: Option[String]): String = {

    val locationStmt = if (location.isDefined) {
      s"Location '${location.get}'" + "\n"
    } else {
      s""
    }

    if (partitionColumns.nonEmpty) {
      val fieldsMap = fields.toMap
      "CREATE TABLE `" +
        tableName.split("\\.").mkString("`.`") + "` (\n\t" +
        (fields.filterNot(x => partitionColumns.contains(x._1)) ++ partitionColumns.map(
          col => (col, fieldsMap(col))))
          .map(z => {
            z._1 + " " + z._2
          })
          .mkString(", \n\t") +
        "\n)  " + sparkFormatSpecifier + " partitioned by (" + partitionColumns.mkString(
        ",") +
        ") \n" + locationStmt + sparkTableProperties
    } else {
      "CREATE TABLE `" +
        tableName.split("\\.").mkString("`.`") + "` (\n\t" + fields
        .map(z => {
          z._1 + " " + z._2
        })
        .mkString(", \n\t") + ") \n " + sparkFormatSpecifier + " \n " + locationStmt + sparkTableProperties
    }
  }

  def getCreateHiveTableDDL(
      tableName: String,
      fields: Array[(String, String)],
      location: Option[String]): String = {

    val locationStmt = if (location.isDefined) {
      s"Location '${location.get}'" + "\n"
    } else {
      s""
    }

    if (partitionColumns.nonEmpty) {
      val fieldsMap = fields.toMap
      "CREATE TABLE `" +
        tableName.split("\\.").mkString("`.`") + "` (\n\t" +
        fields
          .filterNot { x => partitionColumns.contains(x._1) }
          .map(z => {
            z._1 + " " + z._2
          })
          .mkString(", \n\t") +
        "\n) partitioned by (" + partitionColumns
        .map(col => col + " " + fieldsMap(col))
        .mkString(", \n\t") +
        ") \n" + hiveFormatSpecifier + " \n " + locationStmt + hiveTableProperties
    } else {
      "CREATE TABLE `" +
        tableName.split("\\.").mkString("`.`") + "` (\n\t" + fields
        .map(z => {
          z._1 + " " + z._2
        })
        .mkString(", \n\t") + ") \n" + hiveFormatSpecifier + " \n " + locationStmt + hiveTableProperties
    }
  }

  /**
   *
   * @param tableFullName
   * @param backupVersion
   * @param targetTable
   */
  def copyDataFromBackUpTable(
      targetTableName: String,
      backUpTableName: String,
      backUpTableDF: Option[DataFrame] = None,
      failOnAutoBackUpFailure: Boolean = false): Unit = {

    hiveTableUtils.repairTable(backUpTableName)

    val backUpDF = backUpTableDF.getOrElse(sparkSession.table(backUpTableName))

    val targetSchema = sparkSession.table(targetTableName).schema

    // Create a New Data Frame consisting all the columns of new table with new columns as Null (insert into target select column1,column2,null as new_column3 from autobackup)
    val newColumns = targetSchema.fields.map(_.name).diff(backUpDF.columns)
    val restoredFrame = newColumns.foldLeft(backUpDF)((df, column) => {
      val columnType = targetSchema.fields
        .filter(x => {
          x.name == column
        })
        .head
        .dataType
      df.withColumn(column, lit(null).cast(columnType))
    })

    // Restoring old data from backed up table
    logger.info(
      s"Restoring data from backup table $backUpTableName into $targetTableName ...")

    val bkpTableSize = hiveTableUtils.getTableSizeOnDisk(backUpTableName)

    try {
      optimizeForS3WriteAndInsertData(
        tableFullName         = targetTableName,
        dataFrame             = restoredFrame,
        schema                = targetSchema,
        dataSizeOnDiskInBytes = Some(bkpTableSize))
    } catch {
      case e: Exception => {
        if (failOnAutoBackUpFailure) {
          logger.error(
            s"HIVE_WRITER_AUTO_BACKUP:failure while during auto backup from $backUpTableName to $targetTableName",
            e)
          throw e
        } else {
          logger.warn(
            s"HIVE_WRITER_AUTO_BACKUP: failure while during auto backup from $backUpTableName to $targetTableName",
            e)
        }
      }
    }

  }

  /**
   * get version from exiting table or by converting an unversioned table to versioned
   *
   * @param tableFullName
   * @param currentSchema
   * @return
   */
  def getLatestTableVersion(
      tableFullName: String,
      currentSchema: StructType): Option[Int] = {

    // check if any versioned tables exist. get the latestVersion if one exists
    // the below operation is case insensitive
    val versionedTableNamesMap: Map[Int, String] =
      getVersionedTableNamesMap(tableFullName)(sparkSession)

    val tableExists = sparkSession.catalog.tableExists(tableFullName)

    val currentTableVersion = if (versionedTableNamesMap.nonEmpty) {
      val latestVersion =
        versionedTableNamesMap.keys.toList.sorted(Ordering.Int.reverse).head
      val latestVersionedTableName = versionedTableNamesMap(latestVersion)

      if (!tableExists) {
        val versionedTableLocation =
          hiveTableUtils.getTableMetaData(latestVersionedTableName).storage.locationUri
        // create external table and not a view for use in sqls
        createTable(tableFullName, currentSchema, versionedTableLocation)
        hiveTableUtils.repairTable(tableFullName)
      }
      Some(latestVersion)
    } else if (tableExists) {
      val versionedTableName = tableFullName + "_v1"
      // create a versioned table in current Location with existing schema and make the current table external
      val unVersionedTableLocation =
        hiveTableUtils.getTableMetaData(tableFullName).storage.locationUri
      val existingSchema = sparkSession.table(tableFullName).schema
      createTable(versionedTableName, existingSchema, unVersionedTableLocation)
      hiveTableUtils.convertTableType(versionedTableName, CatalogTableType.MANAGED)
      hiveTableUtils.convertTableType(tableFullName, CatalogTableType.EXTERNAL)
      Some(1)
    } else {
      None
    }
    currentTableVersion
  }

  /**
   * handle the table for schema running auto backup
   *
   * @param tableFullName
   * @param inputSchema
   * @return
   */
  def detectAndHandleSchemaChange(
      tableFullName: String,
      inputSchema: StructType,
      failOnAutoBackUpFailure: Boolean = false): (StructType, Int) = {
    val currentTableVersion =
      getLatestTableVersion(tableFullName, inputSchema)
    val (resolvedTableSchema, tableVersion) = if (currentTableVersion.isDefined) {
      // check if  backup is needed if a table version exists
      val existingSchema = sparkSession.table(tableFullName).schema
      val currentVersion = currentTableVersion.get

      if (hiveTableUtils.simpleSchemaCompare(existingSchema, inputSchema)) {
        logger.warn(
          s"DATA_WRITER_SCHEMA_CHANGE: detected a schema change for table $tableFullName")
        // switch the current table location
        val oldVersionedTableName  = tableFullName + "_v" + currentVersion
        val nextVersion            = currentVersion + 1
        val nextVersionedTableName = tableFullName + "_v" + nextVersion
        val latestSchema           = createTable(nextVersionedTableName, inputSchema)
        val newTableLocation =
          hiveTableUtils.getTableMetaData(nextVersionedTableName).storage.locationUri

        // recreate external table with new schema
        hiveTableUtils.convertTableType(tableFullName, CatalogTableType.EXTERNAL)
        sparkSession.sql(s"DROP TABLE $tableFullName")
        createTable(tableFullName, inputSchema, newTableLocation)

        copyDataFromBackUpTable(
          targetTableName         = nextVersionedTableName,
          backUpTableName         = oldVersionedTableName,
          failOnAutoBackUpFailure = failOnAutoBackUpFailure)
        hiveTableUtils.repairTable(tableFullName)

        (latestSchema, nextVersion)
      } else {
        (existingSchema, currentVersion)
      }

    } else {
      val versionedTableName = tableFullName + "_v1"
      // create a managed versioned table
      createTable(versionedTableName, inputSchema)
      val versionedTableLocation =
        hiveTableUtils.getTableMetaData(versionedTableName).storage.locationUri
      // create external table and not a view for use in sqls
      (createTable(tableFullName, inputSchema, versionedTableLocation), 1)
    }
    (resolvedTableSchema, tableVersion)
  }

  /**
   * checks the table for maintenance i.e schema change, versioning, auto back and cleanup
   * @param tableFullName
   * @param inputSchema
   */
  def checkTableForMaintenance(
      tableFullName: String,
      inputSchema: StructType): Boolean = {
    val checkStatus = checkLatestTableAgainstBackup(tableFullName)
    if (checkStatus.getOrElse(true)) {
      detectAndHandleSchemaChange(
        tableFullName,
        inputSchema,
        failOnAutoBackUpFailure = true)
      true
    } else
      false
  }

  @deprecated("use insertSafely", "20.1.0")
  def insert(
      tableFullName: String,
      dataFrame: DataFrame,
      isOverWrite: Boolean = true): DataCloudDataFrameWriterLog =
    insertSafely(tableFullName, dataFrame, isOverWrite)

  /**
   * Inserts into hive table. safely handling the below cases
   * 1.Creates the hive table if not already existing
   * 2.Manages table versions to handle schema changes
   * 3.Auto inserts data from old format to new incase of schema change
   * 4.handles static partitions while create and insert
   *
   * @param tableFullName
   * @param inputDataFrame
   * @param isOverWrite
   * @return
   */
  def insertSafely(
      tableFullName: String,
      inputDataFrame: DataFrame,
      isOverWrite: Boolean,
      dataSizeOnDiskInBytes: Option[Long] = None): DataCloudDataFrameWriterLog = {

    // renames columns to lower case
    val lowerCasedDf =
      inputDataFrame.toDF(inputDataFrame.columns.map(_.toLowerCase()): _*)

    if (dynamicPartitionColumns.nonEmpty) {
      val lowerCasedColumnNames = lowerCasedDf.columns
      require(
        dynamicPartitionColumns.forall { x =>
          lowerCasedColumnNames.contains(x)
        },
        s"Incorrect partition configuration.PARTITIONS_COLUMNS=$dynamicPartitionColumns DF_COLUMNS=${lowerCasedDf.columns.toList}")
    }

    // add static partitions to the dataframe as constant columns
    val dfWithStaticPartitions = staticPartitionMap.foldLeft(lowerCasedDf)((x, y) => {
      x.withColumn(y._1, lit(y._2))
    })

    // We write data to main table AFTER restoring the backup.
    // This is done intentionally to make sure the data in the latest partition is not accidentally overwritten while restoring backup.
    val (resolvedTableSchema, tableVersion) =
      detectAndHandleSchemaChange(tableFullName, dfWithStaticPartitions.schema, false)

    // patch the partition columns in the order of the existing table to avoid data corruption

    val patchedPartitionColumns = resolvedTableSchema.fields
      .filter(f => partitionColumns.contains(f.name))
      .map(_.name)
      .toList

    val patchedPartitionsString = patchedPartitionColumns.mkString(",")

    // all static partitions are converted to dynamic partitions so send all partitions as dynamic partitions during insert
    val writeLog = optimizeForS3WriteAndInsertData(
      tableFullName,
      dfWithStaticPartitions,
      resolvedTableSchema,
      isOverWrite,
      patchedPartitionsString,
      patchedPartitionColumns,
      patchedPartitionColumns,
      dataSizeOnDiskInBytes)

    // repair the versioned table so that partition data is loaded properly
    hiveTableUtils.repairTable(s"${tableFullName}_v$tableVersion")
    try {
      cleanUpOldTableVersions(tableFullName)
    } catch {
      case e: Exception =>
        logger.warn(s"AUTO_CLEANUP_FAILED: because of ${e.getMessage}", e)
    }

    writeLog
  }

  /**
   *
   *
   * @param tableFullName
   * @param isOverWrite
   * @param sqlPartitionsString
   * @param sqlPartitionColumns
   * @param orderedProjections
   * @param partitionsList
   * @param s3OptimizedDF
   * @param tempTableName
   * @param sqlHint
   * @return
   */
  def writeDfToTable(
      tableFullName: String,
      isOverWrite: Boolean,
      sqlPartitionsString: String,
      sqlPartitionColumns: List[String],
      orderedProjections: Array[String],
      partitionsList: List[String],
      s3OptimizedDF: Dataset[Row],
      tempTableName: String =
        scala.util.Random.alphanumeric.dropWhile(_.isDigit).take(8).mkString,
      sqlHint: Option[String] = None) = {

    // overwrite in spark.sql.sources.partitionOverwriteMode static mode wipes out the entire table
    // so drop the table partitions that are being overwritten and do a normal insert
    // doing a normal insert by dropping existing partition does an append
    val overWriteMode = if (isOverWrite && sqlPartitionColumns.nonEmpty) {
      dropPartitionsBeingOverridden(tableFullName, partitionsList)
      false
    } else {
      isOverWrite
    }

    s3OptimizedDF.createOrReplaceTempView(tempTableName)
    val t0 = System.currentTimeMillis()

    val sql = "INSERT " +
      (if (overWriteMode) "OVERWRITE" else "INTO") +
      " TABLE `" + tableFullName.split("\\.").mkString("`.`") + "`" + {
      if (sqlPartitionColumns.nonEmpty) " PARTITION(" + sqlPartitionsString + ")"
      else ""
    } +
      " select " + sqlHint.getOrElse("") + " " + orderedProjections.mkString(
      ",") + " from " + tempTableName

    logger.info(s"INSERT_SQL: $sql")
    println(s"INSERT_SQL: $sql")
    sparkSession.sql(sql)
    sparkSession.catalog.dropTempView(tempTableName)

    val writeTimeSecs = (System.currentTimeMillis() - t0) / 1000
    logger.info(s"$tableFullName was witten in $writeTimeSecs seconds")
    writeTimeSecs
  }

  /**
   *
   *
   *
   * @param tableFullName
   * @param partitionsList
   */
  def dropPartitionsBeingOverridden(
      tableFullName: String,
      partitionsList: List[String]): Unit = {
    // doing show partitions is easier but on empty table it throws errors
    val existingPartitions = hiveTableUtils.getTablePartitionNames(tableFullName)
    if (existingPartitions.nonEmpty) {
      val isExternal = (sparkSession.catalog
        .getTable(tableFullName)
        .tableType == CatalogTableType.EXTERNAL.name)

      // make a case insensitive comparison but use the correct case in drop sql
      val partitionsListLowerCase = partitionsList.map(_.toLowerCase())

      val overWritingPartitions =
        existingPartitions.filter(x => partitionsListLowerCase.contains(x.toLowerCase))
      if (overWritingPartitions.nonEmpty) {
        val partitionDropSql =
          hiveTableUtils.getDropPartitionsSql(tableFullName, overWritingPartitions)

        logger.info(s"PARTITION_DROP_SQL: $partitionDropSql")
        println(s"PARTITION_DROP_SQL: $partitionDropSql")

        // drop partition deletes data only for managed tables. switch an external to managed while dropping partitions
        if (isExternal) {
          hiveTableUtils.convertTableType(tableFullName, CatalogTableType.MANAGED)
        }
        sparkSession.sql(partitionDropSql)
        if (isExternal) {
          hiveTableUtils.convertTableType(tableFullName, CatalogTableType.EXTERNAL)
        }
      }
    }
  }

  /**
   * This method is invoked from sql wrapper also and so any exception is directly thrown without handling it.
   * to help identify any mistakes in the input sql
   *
   * @param tableFullName
   * @param dataFrame
   * @param schema
   * @param isOverWrite
   * @param sqlPartitionsString
   * @param sqlPartitionColumns
   * @param sqlDynamicPartitionColumns
   * @param dataSizeOnDiskInBytes
   * @return
   */
  def optimizeForS3WriteAndInsertData(
      tableFullName: String,
      dataFrame: DataFrame,
      schema: StructType,
      isOverWrite: Boolean                     = true,
      sqlPartitionsString: String              = partitionsString,
      sqlPartitionColumns: List[String]        = partitionColumns,
      sqlDynamicPartitionColumns: List[String] = dynamicPartitionColumns,
      dataSizeOnDiskInBytes: Option[Long]      = None): DataCloudDataFrameWriterLog = {
    logger.info(
      s"INSERT_INTO_TABLE_CONFIG: writing to table " + tableFullName
        + " with config \nPARTITION_COLUMNS=" + sqlPartitionColumns
        + "\nSTATIC_PARTITONS=" + staticPartitionMap
        + "\nDYNAMIC_PARTITONS=" + sqlDynamicPartitionColumns
        + "\nDISTRIBUTE_BY_COLUMNS=" + addonDistributionColumns
        + "\nSQL_PARTITIONS_STRING=" + sqlPartitionsString)

    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkSession.conf
      .set(
        "hive.exec.max.dynamic.partitions",
        "5000"
      ) // set a high enough upper bound for number of dynamic partitions

    // EMRFS S3-optimized Committer only works with static partition mode
    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    checkPartitionColumnsOrderForInsert(tableFullName, sqlPartitionColumns)
    val sc     = sparkSession.sparkContext
    val fields = getColumnDefinitions(schema)

    val orderedProjections =
      fields.filterNot(x => sqlDynamicPartitionColumns.contains(x._1)).map {
        _._1.toLowerCase()
      } ++ sqlDynamicPartitionColumns

    // generate unique suffix for custom columns to avoid name collisions
    val randomColPrefix =
      scala.util.Random.alphanumeric.dropWhile(_.isDigit).take(8).mkString
    val partitionStringColname: String = randomColPrefix + "_partition_name"

    val customPartitionColumn: Column = if (sqlPartitionColumns.isEmpty) {
      lit("NA")
    } else {
      val partitionConfigString = sqlPartitionsString.replaceAll("'", "")
      logger.info(s"PARTITION_CONFIG_STRING: $partitionConfigString")
      concat_ws(
        "/",
        partitionConfigString
          .split(",")
          .map(x => {
            if (x.contains("=")) lit(x)
            else concat_ws("=", lit(s"$x"), col(x))
          }): _*)
    }

    // generate a single column for partition combo
    val replacedDf = replaceEmptyFieldsWithNull(dataFrame)
      .withColumn(partitionStringColname, customPartitionColumn)

    val tempTableName =
      scala.util.Random.alphanumeric.dropWhile(_.isDigit).take(8).mkString

    // Persist it to avoid re-computation before write to hive
    val persistedDF = replacedDf.persist(StorageLevel.DISK_ONLY)

    try {
      val (s3df: DataFrame, dfSize: Long) = dataSizeOnDiskInBytes match {
        case Some(x) => (replacedDf, x)
        case _ => {
          hiveTableUtils.estimatePersistedDFSizeOnFS(persistedDF)
        }
      }

      if (dfSize == 0) {
        DataCloudDataFrameWriterLog(
          sc.applicationId,
          sc.applicationAttemptId,
          tableFullName,
          0,
          0,
          0,
          0,
          "empty dataframe skipping write")
      } else {
        val partitionsList =
          s3df.select(partitionStringColname).distinct.collect.map(_.getString(0)).toList

        // number of partitions for the table that is getting written into hive
        val numHivePartitions: Long = if (partitionsList.nonEmpty) {
          partitionsList.size
        } else {
          logger.warn(
            s"partitions by partitionStringColname: $partitionStringColname count is 0 so defaulting it to 1")
          1
        }

        val dataFrameSizeInMBs = dfSize / 1000000 // size in MB

        logger.info(
          s"Dataframe Size for the table: $tableFullName  is $dataFrameSizeInMBs MB")
        logger.info(s"number of hive partitions to be written:  $numHivePartitions")

        // calculate optimal number of files written by distributing the total dataframe size into partitions of approximately 150MB .
        val optimalNumPartitions = Math.max(
          Math.ceil((dataFrameSizeInMBs.toFloat / numHivePartitions) / 150).toInt,
          1)
        logger.info(
          s"OPTIMAL_NUMBER of df partitions calculated are per table partition (approx 200mb per file):  $optimalNumPartitions")

        val totalBucketsCount = partitionsList.size * optimalNumPartitions

        val writeTimeSecs = writeDfToTable(
          tableFullName,
          isOverWrite,
          sqlPartitionsString,
          sqlPartitionColumns,
          orderedProjections,
          partitionsList,
          s3df,
          tempTableName,
          Some(s"/*+ REPARTITION($optimalNumPartitions) */"))

        DataCloudDataFrameWriterLog(
          sc.applicationId,
          sc.applicationAttemptId,
          tableFullName,
          dataFrameSizeInMBs.toInt,
          numHivePartitions.toInt,
          totalBucketsCount,
          writeTimeSecs.toInt)

      }
    } finally {
      persistedDF.unpersist()
    }
  }

  private def estimateDFSizeByWritingToHdfs(
      df: DataFrame,
      hdfsPathForTempWrite: String,
      sqlDynamicPartitionColumns: List[String] = List()) = {
    // write to a temp location to calculate the size on disk
    // TODO: write in the target table format i.e text/json etc and compression i.e snappy/gz etc
    if (sqlDynamicPartitionColumns.nonEmpty) {
      df.write
        .mode(SaveMode.Append)
        .partitionBy(sqlDynamicPartitionColumns: _*)
        .parquet(hdfsPathForTempWrite)
    } else {
      df.write.mode(SaveMode.Append).parquet(hdfsPathForTempWrite)
    }
    val sizeOnHDFS = hiveTableUtils.calculateDataSizeFromPath(hdfsPathForTempWrite)
    val dfFromPath = sparkSession.read.parquet(hdfsPathForTempWrite)
    (dfFromPath, sizeOnHDFS)
  }

  /**
   * Overwrites into a hive table. Creates the hive table if not already existing
   */
  def insertOverwrite(tableName: String, dataFrame: DataFrame): DataCloudDataFrameWriterLog =
    insertSafely(tableName, dataFrame, isOverWrite = true)

  def createView(targetDb: String, targetTable: String, query: String) {
    val sql = "CREATE OR REPLACE VIEW " + targetDb + "." + targetTable + " AS " + query
    logger.info(sql)
    sparkSession.sql(sql)
  }

  def truncateTable(dbName: String, tableName: String) {
    val sql = "TRUNCATE TABLE " + dbName + "." + tableName
    logger.info(sql)
    sparkSession.sql(sql)
  }

  /**
   * avoid data corruption by adding check on partition columns insert order
   *
   * @param tableFullName
   * @param columns
   */
  def checkPartitionColumnsOrderForInsert(tableFullName: String, columns: List[String]) {
    val tableIdentifier =
      sparkSession.sessionState.sqlParser.parseTableIdentifier(tableFullName)
    val tablePartitionColumnsOrder = sparkSession.sessionState.catalog
      .getTableMetadata(tableIdentifier)
      .partitionColumnNames
      .mkString(",")
      .toLowerCase()
    val insertPartitionColumnsOrder = columns.mkString(",").toLowerCase()

    require(
      tablePartitionColumnsOrder == insertPartitionColumnsOrder,
      s"$tableFullName Partition Columns order: $tablePartitionColumnsOrder doesn't match insert order: $insertPartitionColumnsOrder")
  }

  /**
   * this is a work around to handle HIVE-11625
   *
   * @see <a href="https://issues.apache.org/jira/browse/HIVE-11625">HIVE-11625</a>
   */
  def replaceEmptyFieldsWithNull(dataframe: DataFrame): DataFrame = {
    val fieldsList = dataframe.schema.toList
    val replacedDf = fieldsList.foldLeft(dataframe)((df, fieldType) =>
      fieldType.dataType match {
        case MapType(_, _, _) | ArrayType(_, _) =>
          df.withColumn(
            fieldType.name,
            expr(
              s"CASE WHEN SIZE(${fieldType.name}) = 0 THEN NULL ELSE ${fieldType.name} END"))
        case _ => df
      })
    replacedDf
  }

}

// Companion object
object HiveDataFrameWriter {

  private val logger = Logger.getLogger(getClass())

  // Factory method
  def apply(
      format: String,
      partitionSpec: Option[String],
      addonDistributionSpec: Option[String] = None)(implicit
      sparkSession: SparkSession): HiveDataFrameWriter = {

    val addonDistributionColumnList = addonDistributionSpec match {
      case Some(x) => x.split(",").toList
      case None    => List[String]()
    }

    partitionSpec match {
      case Some(partitionSpecString) => {

        // preserve order of columns from source
        val partitionColumns: List[String] =
          if (partitionSpecString.nonEmpty)
            partitionSpecString
              .split(",")
              .map(token => {
                if (token.contains("=")) {
                  token.split("=")(0)
                } else {
                  token
                }
              })
              .toList
              .map(_.toLowerCase())
          else
            List()

        val staticPartitionMap: Map[String, String] =
          if (partitionSpecString.split("=").length >= 2)
            partitionSpecString
              .split(",")
              .filter(_.contains("="))
              .map(y => y.split("=")(0).toLowerCase() -> y.split("=")(1))
              .toMap
          else Map[String, String]()

        val partitionsColString: String = partitionSpecString
          .split(",")
          .map(token => {
            if (token.contains("=")) {
              val params = token.split("=")
              params(0) + "='" + params(1) + "'"
            } else token
          })
          .mkString(",")

        format match {
          case "parquet" =>
            new ParquetHiveDataFrameWriter(
              partitionsColString,
              partitionColumns,
              staticPartitionMap,
              addonDistributionColumnList)
          case "text" =>
            new DelimitedHiveDataFrameWriter(
              '\t',
              partitionsColString,
              partitionColumns,
              staticPartitionMap,
              addonDistributionColumnList)
        }
      }
      case None => {
        format match {
          case "parquet" =>
            new ParquetHiveDataFrameWriter(
              addonDistributionColumns = addonDistributionColumnList)
          case "text" =>
            new DelimitedHiveDataFrameWriter(
              separator                = '\t',
              addonDistributionColumns = addonDistributionColumnList)
        }
      }
    }

  }

  def getVersionedTableNamesMap(tableFullName: String)(implicit
      sparkSession: SparkSession): Map[Int, String] = {

    val versionNumberRegex = ("(?i)" + tableFullName + "_v(\\d+)$").r
    val tableIdentifier =
      sparkSession.sessionState.sqlParser.parseTableIdentifier(tableFullName)

    val db = if (tableIdentifier.database.isDefined) {
      tableIdentifier.database.get
    } else {
      s"default"
    }
    val tableName = tableIdentifier.table

    logger.info(
      s"GET_TABLE_VERSIONS: fetching versions for the table $tableName in the database $db")

    val versionedTableNames = sparkSession.sessionState.catalog
      .listTables(db, tableName + "_v*")
      .map(_.unquotedString.toLowerCase.trim)
      .filter(_.matches(versionNumberRegex.regex))

    logger.info(
      s"GET_TABLE_VERSIONS: fetched versioned tables ${versionedTableNames.toList} for the table $tableName in the database $db")

    versionedTableNames
      .map(versionedTableName => {
        val versionNumberRegex(version) = versionedTableName
        version.toInt -> versionedTableName
      })
      .toMap
  }

  def dropAllTableVersions(tableFullName: String)(implicit
      sparkSession: SparkSession): List[String] = {
    val tableVersionsNames =
      getVersionedTableNamesMap(tableFullName).values
    tableVersionsNames
      .flatMap(versionedTableName => {
        try {
          logger.info(s"dropping table $versionedTableName")
          sparkSession.sql(s"DROP TABLE IF EXISTS $versionedTableName")
          None
        } catch {
          case e: Exception => {
            logger.error(s"error while dropping the table $versionedTableName", e)
            Some(tableFullName)
          }
        }
      })
      .toList
  }

  def cleanUpOldTableVersions(tableFullName: String, cleanUpDepth: Option[Int] = None)(
      implicit sparkSession: SparkSession) = {
    val versionedTableNamesMap: Map[Int, String] = getVersionedTableNamesMap(
      tableFullName)

    // get the versions in increasing order with latest version at then end
    // the below operation is case insensitive
    val tableVersions = versionedTableNamesMap.keys.toList.sorted(Ordering.Int.reverse)

    // all but the latest one are backup tables
    val backupTableNames = tableVersions.tail.map(versionedTableNamesMap)

    if (backupTableNames.length > 1) {
      backupTableNames.indices.reverse.foreach(idx => {
        if (idx == 0) {
          logger.info(
            s"AUTO_CLEANUP: retaining the latest backup table: ${backupTableNames(idx)}")
        } else {
          val sourceBackupTableName = backupTableNames(idx)
          val targetBackupTableName = backupTableNames(idx - 1)
          logger.info(
            s"AUTO_CLEANUP: comparing an old backup table $sourceBackupTableName to its next version $targetBackupTableName")
          if (compareAutoBackupTablesData(
              sourceBackupTable = sourceBackupTableName,
              targetBackupTable = targetBackupTableName)
              .getOrElse(false)) {
            logger.info(
              s"AUTO_CLEANUP: dropping an old backup table $sourceBackupTableName after comparing it with $targetBackupTableName")
            sparkSession.sql(s"Drop table ${backupTableNames(idx)}")
          } else {
            logger.warn(
              s"AUTO_CLEANUP: unable to auto compare table $sourceBackupTableName with $targetBackupTableName please do a manual compare and drop")
            throw new Exception(
              s"unable to auto compare table $sourceBackupTableName with $targetBackupTableName")
          }
        }
      })
    }
  }

  def compareAutoBackupTablesData(sourceBackupTable: String, targetBackupTable: String)(
      implicit sparkSession: SparkSession): Option[Boolean] = {

    logger.info(
      s"BACKUP_TABLE_COMPARISON: comparing $targetBackupTable with backup table $sourceBackupTable")
    val sourcePartitionColumns =
      hiveTableUtils
        .getTableMetaData(sourceBackupTable)
        .partitionColumnNames

    val sourcePartitionColumnsString = sourcePartitionColumns
      .map(_.toLowerCase())
      .mkString(",")

    val targetPartitionColumns =
      hiveTableUtils
        .getTableMetaData(targetBackupTable)
        .partitionColumnNames

    val targetPartitionColumnsString = targetPartitionColumns
      .map(_.toLowerCase())
      .mkString(",")

    if (sourcePartitionColumnsString == targetPartitionColumnsString) {
      logger.info(
        s"BACKUP_TABLE_COMPARISON: since partitions of $targetBackupTable and $sourceBackupTable match tables can be compared")
      compareTablesWithSamePartitioningConfig(sourceBackupTable, targetBackupTable)
    } else if (sourcePartitionColumnsString.isEmpty & targetPartitionColumnsString.isEmpty) {
      val sourceBackUpDF = sparkSession.table(sourceBackupTable)
      val targetBackUpDf = sparkSession.table(targetBackupTable)
      if (!targetBackUpDf.isEmpty & sourceBackUpDF.isEmpty) {
        Some(false)
      } else
        Some(true)
    } else {
      logger.warn(
        s"BACKUP_TABLE_COMPARISON: $targetBackupTable and $sourceBackupTable cannot be compared")
      // returning None as comparison can't be done
      None
    }
  }

  @tailrec
  private def compareTablesWithSamePartitioningConfig(
      sourceBackupTable: String,
      targetBackupTable: String,
      repairTables: Boolean = false)(implicit
      sparkSession: SparkSession): Option[Boolean] = {

    //repairing table is a costly operation try it only if a simple comparison yields false

    if (repairTables) {
      logger.info(
        s"BACKUP_TABLE_COMPARISON: repairing tables to recover partitions from $targetBackupTable and $sourceBackupTable")
      hiveTableUtils.repairTable(targetBackupTable)
      hiveTableUtils.repairTable(sourceBackupTable)
    }
    val sourcePartitions =
      hiveTableUtils.getTablePartitionNames(sourceBackupTable).toSet
    val targetPartitions =
      hiveTableUtils.getTablePartitionNames(targetBackupTable).toSet
    val status = Some(sourcePartitions.subsetOf(targetPartitions))

    // if tables have been already repaired then return the status or do one more pass by invoking a repair
    if (repairTables) {
      status
    } else {
      logger.warn(
        s"BACKUP_TABLE_COMPARISON: found missing partitions in $targetBackupTable when compared to  $sourceBackupTable doing a second pass of comparison by repairing the tables")
      compareTablesWithSamePartitioningConfig(sourceBackupTable, targetBackupTable, true)
    }
  }

  def checkLatestTableAgainstBackup(tableFullName: String)(implicit
      sparkSession: SparkSession): Option[Boolean] = {
    val versionedTableNamesMap = getVersionedTableNamesMap(tableFullName)
    val tableVersions          = versionedTableNamesMap.keys.toList.sorted(Ordering.Int.reverse)
    if (tableVersions.size > 1) {
      val currentTableName = versionedTableNamesMap(tableVersions(0))
      val backUpTableName  = versionedTableNamesMap(tableVersions(1))
      compareAutoBackupTablesData(
        sourceBackupTable = backUpTableName,
        targetBackupTable = currentTableName)
    } else
      Some(true)
  }

}
