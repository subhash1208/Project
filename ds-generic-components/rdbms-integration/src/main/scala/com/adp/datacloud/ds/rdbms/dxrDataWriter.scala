package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.DxrParallelDataExtractorConfig
import com.adp.datacloud.ds.hive.hiveTableUtils
import com.adp.datacloud.ds.util.DXRLogger
import com.adp.datacloud.ds.utils.dxrHasher
import com.adp.datacloud.writers.{
  DataCloudDataFrameWriterLog,
  DeltaDataFrameWriter,
  HiveDataFrameWriter
}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{PrintWriter, StringWriter}
import scala.collection.parallel.ForkJoinTaskSupport

object dxrDataWriter {
  private val logger = Logger.getLogger(getClass)

  val hashUdf: UserDefinedFunction =
    udf[String, String]((pwd: String) => if (pwd != null) dxrHasher.hash(pwd) else null)

  /**
   * Writes data to Hive Tables in Landing/Blue and Green zones after applying necessary restrictions (like hashing/optout etc...)
   *
   * @param config
   * @param pathsMap
   * @param rowCountsMap
   * @param addonDistributionColumns
   * @param dxrLogger
   * @param skipStagingCleanup
   * @param parallelHiveWrites
   * @param isOverWrite
   * @param skipRowCountValidation
   * @param sparkSession
   * @return
   */
  def writeToHiveTables(
                         config: DxrParallelDataExtractorConfig,
                         pathsMap: Map[String, String],
                         rowCountsMap: Map[String, Long],
                         addonDistributionColumns: Option[String] = None,
                         dxrLogger: DXRLogger,
                         skipStagingCleanup: Boolean,
                         parallelHiveWrites: Int = 2,
                         isOverWrite: Boolean,
                         isVersioned: Boolean = false,
                         skipRowCountValidation: Boolean = false)(implicit
                                                                  sparkSession: SparkSession): List[String] = {

    val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration

    val fileSystem = FileSystem.get(
      config.s3StagingBucket match {
        case Some(x) => new java.net.URI(s"s3://${x}") // Use an S3 bucket when provided
        case None =>
          new java.net.URI(
            hadoopConfiguration.get("fs.default.name")
          ) // Use HDFS if none provided
      },
      hadoopConfiguration)

    val fileSystemPrefix = fileSystem.getUri.toString()

    // parallelize Map
    val pathsMapParllelised = pathsMap.par
    pathsMapParllelised.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(parallelHiveWrites))

    val optoutMasterDf = getOptOutMasterDf(config.optoutMaster)

    val writeFailures = pathsMapParllelised.flatMap {
      case (sqlName, path) =>
        try {
          val extractRowCount: Long = rowCountsMap(sqlName)
          if (skipRowCountValidation || extractRowCount > 0) {
            val dataDf =
              sparkSession.read.parquet(
                s"$fileSystemPrefix$path/*"
              ) // We need to read the paths back from chosen filesystem
            val finalDf = dxrDataWriter.replaceNulls(dataDf, config.nullStrings)
            val dataSize =
              hiveTableUtils.calculateDataSizeFromPath(s"$fileSystemPrefix$path/")

            val extractRowsCount =
              if (skipRowCountValidation) None else Some(extractRowCount)

            dxrDataWriter.saveToHive(
              dataDf                   = finalDf,
              outputDatabaseTable      = sqlName,
              landingDatabaseName      = config.landingDatabaseName,
              blueDatabaseName         = config.blueDatabaseName,
              greenDatabaseName        = config.greenDatabaseName,
              optOutDf                 = optoutMasterDf,
              dxrLogger                = Some(dxrLogger),
              dataSizeOnDiskInBytes    = Some(dataSize),
              extractRowsCount         = extractRowsCount,
              outputFormat             = config.outputFormat,
              outputPartitionSpec      = config.outputPartitionSpec,
              addonDistributionColumns = addonDistributionColumns,
              extraHashColumnsList     = config.extraHashColumnsList,
              isOverWrite              = isOverWrite,
              isVersioned              = isVersioned)
          }
          if (!skipStagingCleanup) {
            fileSystem.delete(new Path(path), true)
          }
          None
        } catch {
          case e: Exception =>
            logger.error(s"failed writing the table $sqlName to hive: ", e)
            val sw = new StringWriter
            e.printStackTrace(new PrintWriter(sw))
            dxrLogger.insertHiveDfWriterLog(
              DataCloudDataFrameWriterLog(
                sparkSession.sparkContext.applicationId,
                sparkSession.sparkContext.applicationAttemptId,
                sqlName,
                0,
                0,
                0,
                0,
                sw.toString,
                true))
            Some(sqlName)
        }
    }
    writeFailures.toList
  }

  def getOptOutMasterDf(optOutMasterTableName: Option[String])(implicit
                                                               sparkSession: SparkSession) = {
    // Should always have projection while selecting from optout table
    optOutMasterTableName match {
      case Some(x) =>
        Some(sparkSession.sql(
          "select region_code,company_code,product_code,ooid,clnt_obj_id,iid from " + x))
      case None => None
    }
  }

  // Function which applies optout logic and returns an opted out df.
  // No effect when optoutMaster is not available, or if there are no columns in common with optout master
  def getOptedOutDf(
                     actualDf: DataFrame,
                     optOutMasterDf: Option[DataFrame] = None): DataFrame = {
    optOutMasterDf match {
      case Some(x) =>
        val commonColumns = actualDf.columns.intersect(x.columns)
        if (commonColumns.isEmpty)
          actualDf
        else {
          val ooDf = x
            .select(commonColumns.head, commonColumns.tail: _*)
            .distinct()
            .filter(commonColumns.foldLeft(lit(true))({ (a, b) =>
              a.and(col(b).isNotNull.and(col(b).notEqual(lit(""))))
            }))
          val broadcastOoDf = broadcast(ooDf)
          actualDf
            .join(
              broadcastOoDf,
              commonColumns.foldLeft(lit(true)) { (x, y) =>
                x.and(actualDf(y) === broadcastOoDf(y))
              },
              "left_outer")
            .where({
              commonColumns.foldLeft(lit(true))({ (x, y) =>
                x.and(broadcastOoDf(y).isNull)
              })
            })
            .select(actualDf.columns.map { x => actualDf(x) }: _*)
        }
      case None => actualDf
    }
  }

  /**
   * Internal method to replace some strings with literal NULLs
   */
  def replaceNulls(dataFrame: DataFrame, nullStrings: List[String]): DataFrame = {
    if (nullStrings.isEmpty)
      dataFrame
    // Iterate through all string fields and replace the null string with literal null
    dataFrame.schema.fields.foldLeft(dataFrame)({ (df, y) =>
      if (y.dataType == StringType)
        df.withColumn(
          y.name,
          when(
            col(y.name).isin(nullStrings map {
              lit(_)
            }: _*),
            lit(null.asInstanceOf[String])).otherwise(col(y.name)))
      else
        df
    })
  }

//  def quickSaveToHive(
//                       dataDf: DataFrame,
//                       outputDatabaseTable: String,
//                       landingDatabaseName: String,
//                       blueDatabaseName: Option[String],
//                       greenDatabaseName: Option[String],
//                       optOutMasterTableName: Option[String],
//                       outputPartitionSpec: Option[String] = None,
//                       outputFormat: String                = "delta",
//                       isOverWrite: Boolean                = true)(implicit sparkSession: SparkSession): Unit = {
//
//    val optOutDf = getOptOutMasterDf(optOutMasterTableName)
//
//    saveToHive(
//      dataDf              = dataDf,
//      outputDatabaseTable = outputDatabaseTable,
//      landingDatabaseName = landingDatabaseName,
//      blueDatabaseName    = blueDatabaseName,
//      greenDatabaseName   = greenDatabaseName,
//      optOutDf            = optOutDf,
//      outputFormat        = outputFormat,
//      outputPartitionSpec = outputPartitionSpec,
//      isOverWrite         = isOverWrite)
//  }

  def saveToHive(
                  dataDf: DataFrame,
                  outputDatabaseTable: String,
                  landingDatabaseName: String,
                  blueDatabaseName: Option[String],
                  greenDatabaseName: Option[String],
                  optOutDf: Option[DataFrame],
                  outputFormat: String,
                  dxrLogger: Option[DXRLogger]        = None,
                  dataSizeOnDiskInBytes: Option[Long] = None,
                  extractRowsCount: Option[Long]      = None,
                  outputPartitionSpec: Option[String],
                  addonDistributionColumns: Option[String] = None,
                  extraHashColumnsList: List[String]       = List(),
                  isOverWrite: Boolean,
                  isVersioned: Boolean)(implicit sparkSession: SparkSession): Unit = {

    val finalDf =
      dataDf.columns.foldRight(dataDf)({ (x, y) =>
        y.withColumnRenamed(x, x.toLowerCase())
      }) // Oracle returns column names in upper case

    val strictHashedDf = dxrDataWriter.saveToLandingDB(
      databaseName             = landingDatabaseName,
      dataDf                   = finalDf,
      outputDatabaseTable      = outputDatabaseTable,
      dxrLogger                = dxrLogger,
      dataSizeOnDiskInBytes    = dataSizeOnDiskInBytes,
      extractRowsCount         = extractRowsCount,
      outputFormat             = outputFormat,
      outputPartitionSpec      = outputPartitionSpec,
      addonDistributionColumns = addonDistributionColumns,
      isOverWrite              = isOverWrite,
      isVersioned             = isVersioned)

    val optedOutDf = dxrDataWriter.saveToBlueDB(
      blueDatabaseName         = blueDatabaseName,
      strictHashedDf           = strictHashedDf,
      optoutDf                 = optOutDf,
      outputDatabaseTable      = outputDatabaseTable,
      dxrLogger                = dxrLogger,
      dataSizeOnDiskInBytes    = dataSizeOnDiskInBytes,
      extractRowsCount         = extractRowsCount,
      outputFormat             = outputFormat,
      outputPartitionSpec      = outputPartitionSpec,
      addonDistributionColumns = addonDistributionColumns,
      isOverWrite              = isOverWrite,
      isVersioned              = isVersioned)

    dxrDataWriter.saveToGreenDB(
      greenDatabaseName        = greenDatabaseName,
      optedOutDf               = optedOutDf,
      outputDatabaseTable      = outputDatabaseTable,
      dxrLogger                = dxrLogger,
      dataSizeOnDiskInBytes    = dataSizeOnDiskInBytes,
      extractRowsCount         = extractRowsCount,
      outputFormat             = outputFormat,
      outputPartitionSpec      = outputPartitionSpec,
      addonDistributionColumns = addonDistributionColumns,
      extraHashColumnsList     = extraHashColumnsList,
      isOverWrite              = isOverWrite,
      isVersioned              = isVersioned)

    logger.info(s"All writes completed for $outputDatabaseTable")

  }

  /**
   * saves a copy of data by excluding optout clients and with hashing only on strict hash columns
   *
   * @param blueDatabaseName
   * @param strictHashedDf
   * @param optoutDf
   * @param outputDatabaseTable
   * @param dxrLogger
   * @param dataSizeOnDiskInBytes
   * @param extractRowsCount
   * @param outputFormat
   * @param outputPartitionSpec
   * @param addonDistributionColumns
   * @param isOverWrite
   * @param sparkSession
   * @return
   */
  def saveToBlueDB(
                    blueDatabaseName: Option[String],
                    strictHashedDf: DataFrame,
                    optoutDf: Option[DataFrame],
                    outputDatabaseTable: String,
                    dxrLogger: Option[DXRLogger],
                    dataSizeOnDiskInBytes: Option[Long] = None,
                    extractRowsCount: Option[Long]      = None,
                    outputFormat: String,
                    outputPartitionSpec: Option[String],
                    addonDistributionColumns: Option[String] = None,
                    isOverWrite: Boolean,
                    isVersioned: Boolean)(implicit sparkSession: SparkSession): DataFrame = {

    // Apply Optout filter on landing DB frame
    val optedOutDf = getOptedOutDf(strictHashedDf, optoutDf)

    if (extractRowsCount.isDefined) {
      val blueDBDataCount = optedOutDf.count()
      require(
        blueDBDataCount <= extractRowsCount.get,
        s"Blue DB write rowcount $blueDBDataCount show be <= extracted rowcount ${extractRowsCount.get}")
    }

    blueDatabaseName match {
      case Some(databaseName) => {
        logger.info(s"Writing $outputDatabaseTable to BLUE DB... $databaseName")

        val fullTableName = databaseName + "." + outputDatabaseTable
        val log = if (isVersioned == false) {
          DeltaDataFrameWriter.insert(
            tableName     = fullTableName,
            inputDf       = optedOutDf,
            isOverWrite   = isOverWrite,
            partitionSpec = outputPartitionSpec)
        } else {

          val hiveWriter =
            HiveDataFrameWriter(
              outputFormat,
              outputPartitionSpec,
              addonDistributionColumns)

          hiveWriter.insertSafely(
            fullTableName,
            optedOutDf,
            isOverWrite,
            dataSizeOnDiskInBytes)
        }

        dxrLogger.foreach(_.insertHiveDfWriterLog(log))
      }
      case None =>
        logger.info(
          s"skipping blue db write for $outputDatabaseTable  as it was not provided")
    }
    optedOutDf
  }

  /**
   * saves a copy of data with hashing only on strict hashing columns
   *
   * @param databaseName
   * @param dataDf
   * @param outputDatabaseTable
   * @param dxrLogger
   * @param dataSizeOnDiskInBytes
   * @param extractRowsCount
   * @param outputFormat
   * @param outputPartitionSpec
   * @param addonDistributionColumns
   * @param isOverWrite
   * @param sparkSession
   * @return
   */
  def saveToLandingDB(
                       databaseName: String,
                       dataDf: DataFrame,
                       outputDatabaseTable: String,
                       dxrLogger: Option[DXRLogger],
                       dataSizeOnDiskInBytes: Option[Long] = None,
                       extractRowsCount: Option[Long]      = None,
                       outputFormat: String,
                       outputPartitionSpec: Option[String],
                       addonDistributionColumns: Option[String] = None,
                       isOverWrite: Boolean,
                       isVersioned: Boolean)(implicit sparkSession: SparkSession) = {

    // apply strict hash on sensitive columns irrespective of Green/Blue/landing zone we are loading
    val columnsForStrictHashing = dataDf.columns.filter { x => x.endsWith("_stricthash") }
    val strictHashedDf = columnsForStrictHashing.foldLeft(dataDf)({ (df, column) =>
      df.withColumn(column, hashUdf(col(column)))
    })

    if (extractRowsCount.isDefined) {
      val landingDBDataCount = strictHashedDf.count()
      require(
        landingDBDataCount == extractRowsCount.get,
        s"Landing DB write rowcount $landingDBDataCount doesn't match extracted rowcount ${extractRowsCount.get}")
    }

    logger.info(s"Writing $outputDatabaseTable to LANDING DB... $databaseName")

    val fullTableName = databaseName + "." + outputDatabaseTable
    val log = if (isVersioned == false) {
      DeltaDataFrameWriter.insert(
        tableName     = fullTableName,
        inputDf       = strictHashedDf,
        isOverWrite   = isOverWrite,
        partitionSpec = outputPartitionSpec)
    } else {
      val hiveWriter =
        HiveDataFrameWriter(outputFormat, outputPartitionSpec, addonDistributionColumns)

      // Write all data to blue landing zone DB first

      hiveWriter.insertSafely(
        fullTableName,
        strictHashedDf,
        isOverWrite,
        dataSizeOnDiskInBytes)

    }

    dxrLogger.foreach(_.insertHiveDfWriterLog(log))
    strictHashedDf
  }

  /**
   * saves a copy of data by excluding optout clients and with hashing only on all columns marked for hashing (_hash and _stricthash)
   *
   * @param greenDatabaseName
   * @param optedOutDf
   * @param outputDatabaseTable
   * @param dxrLogger
   * @param dataSizeOnDiskInBytes
   * @param extractRowsCount
   * @param outputFormat
   * @param outputPartitionSpec
   * @param addonDistributionColumns
   * @param isOverWrite
   * @param sparkSession
   * @return
   */
  def saveToGreenDB(
                     greenDatabaseName: Option[String],
                     optedOutDf: DataFrame,
                     outputDatabaseTable: String,
                     dxrLogger: Option[DXRLogger],
                     dataSizeOnDiskInBytes: Option[Long] = None,
                     extractRowsCount: Option[Long]      = None,
                     outputFormat: String,
                     outputPartitionSpec: Option[String],
                     addonDistributionColumns: Option[String] = None,
                     extraHashColumnsList: List[String]       = List(),
                     isOverWrite: Boolean,
                     isVersioned: Boolean)(implicit sparkSession: SparkSession) = {

    // apply one way hash on sensitive columns
    val columnsForHashing = (optedOutDf.columns.filter { x =>
      x.endsWith("_hash")
    } ++ extraHashColumnsList).distinct
    val hashedDf = columnsForHashing.foldLeft(optedOutDf)({ (df, column) =>
      df.withColumn(column, hashUdf(col(column)))
    })

    if (extractRowsCount.isDefined) {
      val greenDBDataCount = hashedDf.count()
      require(
        greenDBDataCount <= extractRowsCount.get,
        s"Green DB write rowcount $greenDBDataCount show be <= extracted rowcount ${extractRowsCount.get}")
    }

    greenDatabaseName match {
      case Some(databaseName) => {
        // Then write to Green Zone
        logger.info(s"Writing $outputDatabaseTable to GREEN DB... $databaseName")

        if (databaseName != "null") {
          val fullTableName = databaseName + "." + outputDatabaseTable
          val log = if (isVersioned == false) {
            DeltaDataFrameWriter.insert(
              tableName     = fullTableName,
              inputDf       = hashedDf,
              isOverWrite   = isOverWrite,
              partitionSpec = outputPartitionSpec)

          } else {

            val hiveWriter =
              HiveDataFrameWriter(
                outputFormat,
                outputPartitionSpec,
                addonDistributionColumns)

            hiveWriter.insertSafely(
              fullTableName,
              hashedDf,
              isOverWrite,
              dataSizeOnDiskInBytes)
          }
          dxrLogger.foreach(_.insertHiveDfWriterLog(log))

        } else {
          logger.info("Do Not write to green db , if green db variable is null")
        }
        logger.info("GREEN DB writes completed")

      }
      case None =>
        logger.info(
          s"skipping green db write for $outputDatabaseTable  as it was not provided")
    }
    hashedDf
  }

  def checkTargetTablesForSchemaChange(
                                        schemaMap: Map[String, StructType],
                                        databaseNames: List[String],
                                        outputPartitionSpec: Option[String],
                                        addonDistributionColumns: Option[String] = None,
                                        outputFormat: String                     = "parquet",
                                        parallelTableWrites: Int                 = 2)(implicit sparkSession: SparkSession): Unit = {

    val hiveWriter =
      HiveDataFrameWriter(outputFormat, outputPartitionSpec, addonDistributionColumns)

    val schemasMapParallelized = schemaMap.par

    schemasMapParallelized.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(parallelTableWrites))

    val statusCheck = databaseNames
      .map(dbname => {
        val tablesStatusCheckList = schemasMapParallelized.flatMap {
          case (tableName, tableSchema) => {
            val tableFullName = s"${dbname}.${tableName}"
            logger.warn(s"DXR_TABLE_CHECKUP: running checkup on $tableFullName")
            try {
              hiveWriter.checkTableForMaintenance(tableFullName, tableSchema)
              None
            } catch {
              case e: Exception => {
                logger.error(
                  s"table $tableFullName failed maintenance check please fix before proceeding",
                  e)
                Some(tableFullName)
              }
            }
          }
        }.toList
        if (tablesStatusCheckList.nonEmpty) {
          Some(tablesStatusCheckList.mkString(","))
        } else None
      })
      .flatten

    require(
      statusCheck.isEmpty,
      s"The tables $statusCheck failed maintenance check please fix before proceeding")
  }

}