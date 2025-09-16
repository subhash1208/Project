package com.adp.datacloud.util

import com.adp.datacloud.cli.{VersionedTablesPatcherConfig, VersionedTablesPatcherOptions}
import com.adp.datacloud.ds.hive.hiveTableUtils
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.collection.parallel.ForkJoinTaskSupport

object VersionedTablesPatcher {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")
    val config: VersionedTablesPatcherConfig = VersionedTablesPatcherOptions.parse(args)
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(config.applicationName)
      .enableHiveSupport()
      .getOrCreate()

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
    runPatching(config)
  }

  def runPatching(config: VersionedTablesPatcherConfig)(implicit
      sparkSession: SparkSession): Unit = {

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    logger.info(s"TABLE_PATCHER: running job with config ${write(config)}")
    val tableNamesParallelized = config.tableNames.par
    tableNamesParallelized.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(2))
    val resp = for (database <- config.databases) yield {
      tableNamesParallelized
        .map(tableName => patchTable(database, tableName, config.depth))
        .toList
        .flatten
    }
    val collectivePatchingStatus = resp.flatten
    require(
      collectivePatchingStatus.isEmpty,
      s"The tables ${collectivePatchingStatus.mkString(",")} seem to have issues during backup please check logs for details ")
  }

  def patchTable(db: String, tableName: String, depth: Int = 0)(implicit
      sparkSession: SparkSession): List[String] = {
    val tableFullName    = db + "." + tableName
    val tableVersionsMap = HiveDataFrameWriter.getVersionedTableNamesMap(tableFullName)
    logger.info(
      s"TABLE_PATCHER: found versions ${tableVersionsMap.values.mkString(",")} processing them with depth $depth")
    if (tableVersionsMap.keys.size > 1) {
      val reverseSortedVersions =
        tableVersionsMap.keys.toList.sorted(Ordering[Int].reverse)
      val maxDepth = if (depth == 0) {
        reverseSortedVersions.length
      } else {
        Math.min(reverseSortedVersions.length, depth)
      }
      val (topVersions, _) = reverseSortedVersions.splitAt(maxDepth)

      // sort them in ascending order to trigger copy from lower versions to higher versions
      val limitedVersions = topVersions.sorted
      logger.info(
        s"TABLE_PATCHER: considering versions $limitedVersions with maxDepth $maxDepth for backup check")
      val resp = for (idx <- 0 until limitedVersions.length) yield {
        val targetTableVersion = limitedVersions(idx)
        val backupTableVersion = targetTableVersion - 1
        val targetTableName    = tableVersionsMap(targetTableVersion)
        val backupTableName    = tableVersionsMap.get(backupTableVersion)

        val status = if (backupTableName.isDefined) {
          HiveDataFrameWriter
            .compareAutoBackupTablesData(
              sourceBackupTable = backupTableName.get,
              targetBackupTable = targetTableName)

        } else {
          logger.warn(
            s"TABLE_PATCHER: skipping backup copy for $targetTableName as no backupTable with version $backupTableVersion found")
          None
        }

        logger.info(
          s"TABLE_PATCHER: result of mismatch comparison between sourceBackupTable:$backupTableName and targetBackupTable:$targetTableName is $status")
        if (status.isDefined && !status.get) {
          try {
            val partitionColumnNames = hiveTableUtils
              .getTableMetaData(targetTableName)
              .partitionColumnNames
              .mkString(",")
            val partitionSpec =
              if (partitionColumnNames.isEmpty) None
              else Some(partitionColumnNames)
            val hiveDataFrameWriter = HiveDataFrameWriter("parquet", partitionSpec)
            hiveDataFrameWriter
              .copyDataFromBackUpTable(
                targetTableName,
                backupTableName.get,
                failOnAutoBackUpFailure = true)
            None
          } catch {
            case e: Exception => {
              logger.error(
                s"Exception while copying data from $backupTableName to $targetTableName",
                e)
              Some(targetTableName)
            }
          }
        } else
          None
      }
      resp.toList.flatten
    } else List.empty[String]

  }
}
