package com.adp.datacloud.ds.hive

import java.net.URI
import org.apache.hadoop.fs.{ContentSummary, FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

object hiveTableUtils {
  private val logger = Logger.getLogger(getClass())

  /**
   * this is a work around to handle SPARK-17657
   *
   * @see <a href="https://issues.apache.org/jira/browse/SPARK-17657">SPARK-17657</a>
   */
  def convertTableType(tableName: String, catlalogTableType: CatalogTableType)(implicit
      sparkSession: SparkSession): Unit = {
    val tableIdentifier =
      sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    val oldTable =
      sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    val alteredTable = oldTable.copy(tableType = catlalogTableType)
    sparkSession.sessionState.catalog.alterTable(alteredTable)
  }

  def getTablePartitionNames(fullTableName: String)(implicit
      sparkSession: SparkSession): Seq[String] = {
    val tableIdentifier =
      sparkSession.sessionState.sqlParser.parseTableIdentifier(fullTableName)
    sparkSession.sessionState.catalog.listPartitionNames(tableIdentifier, None)
  }

  def getTableMetaData(fullTableName: String)(implicit
      sparkSession: SparkSession): CatalogTable = {
    sparkSession.sessionState.catalog.getTableMetadata(
      sparkSession.sessionState.sqlParser.parseTableIdentifier(fullTableName))
  }

  def getTableLocation(fullTableName: String)(implicit
      sparkSession: SparkSession): String = {
    val tableMetaData = getTableMetaData(fullTableName)
    tableMetaData.storage.locationUri.get.toString
  }

  /**
   * For a given table name returns the size on disk in bytes
   *
   * @param fullTableName
   * @param sparkSession
   * @return
   */
  def getTableSizeOnDisk(fullTableName: String)(implicit
      sparkSession: SparkSession): Long = {
    val tableLocation = getTableLocation(fullTableName)
    logger.info(s"calculating $fullTableName size on Disk from path $tableLocation")
    calculateDataSizeFromPath(tableLocation)
  }

  def repairTable(fullTableName: String)(implicit sparkSession: SparkSession): Unit = {
    val tableMetaData = getTableMetaData(fullTableName)
    // run MSCK only if its a partitioned table
    if (tableMetaData.partitionColumnNames.nonEmpty) {
      val repairSql = s"MSCK REPAIR TABLE $fullTableName"
      logger.info(s"REPAIR_SQL:$repairSql")
      sparkSession.sql(repairSql).show()
    }
  }

  /**
   * do a simple schema compare i.e find equals for string or nonstring fields
   *
   * @param oldSchema
   * @param newSchema
   * @return
   */
  def simpleSchemaCompare(oldSchema: StructType, newSchema: StructType): Boolean = {
    val existingSchemaFieldSet = oldSchema.fields
      .map(x => {
        x.name.toLowerCase + (x.dataType.simpleString match {
          case "string" => "string"
          case _        => "nonstring"
        })
      })
      .toSet
    val incomingFieldSet = newSchema.fields
      .map(x => {
        x.name.toLowerCase + (x.dataType.simpleString match {
          case "string" => "string"
          case _        => "nonstring"
        })
      })
      .toSet
    existingSchemaFieldSet != incomingFieldSet
  }

  /**
   * For a given s3/hdfs path returns the size on disk in bytes
   *
   * @param dataPath
   * @param sparkSession
   */
  def calculateDataSizeFromPath(dataPath: String)(implicit
      sparkSession: SparkSession): Long = {
    val hdfsConf                    = sparkSession.sparkContext.hadoopConfiguration
    val fs                          = FileSystem.get(new URI(dataPath), hdfsConf)
    val fsPath                      = new Path(dataPath)
    val pathSummary: ContentSummary = fs.getContentSummary(fsPath)
    pathSummary.getLength
  }

  /**
   * returns the size of a dataframe if its persisted in bytes
   * returns none if the dataframe is not persisted.
   *
   * @param persistedDF
   * @param sparkSession
   * @return
   */
  def calculatePersistedDFSize(persistedDF: DataFrame)(implicit
      sparkSession: SparkSession): Option[BigInt] = {
    if (persistedDF.count() == 0)
      return Some(0)
    val storageLevel = persistedDF.storageLevel
    val isCached: Boolean =
      storageLevel.useMemory || storageLevel.useDisk || storageLevel.useOffHeap
    if (isCached) {
      val catalyst_plan = persistedDF.queryExecution.logical
      val dataFrameSize = sparkSession.sessionState
        .executePlan(catalyst_plan)
        .optimizedPlan
        .stats
        .sizeInBytes
      Some(dataFrameSize)
    } else {
      None
    }
  }

  /**
   * returns an estimate of persisted dataframes size on s3 or hdfs in bytes by calculating its size in memory
   *
   * @param df
   * @param sparkSession
   * @return
   */
  def estimatePersistedDFSizeOnFS(df: DataFrame)(implicit
      sparkSession: SparkSession): (DataFrame, Long) = {
    val sizeInSparkCache = hiveTableUtils.calculatePersistedDFSize(df).get
    // The final file created is approx 4 times smaller in size due to compression. divide by a factor of 4 to estimate the actual size on s3 or hdfs.
    val estimatedSize = (sizeInSparkCache / 4).toLong
    (df, estimatedSize)
  }

  /**
   * deletes files from given hdfs/s3 path recursively
   *
   * @param targetPath
   * @param sparkSession
   * @return
   */
  def deletePath(targetPath: String)(implicit sparkSession: SparkSession): Boolean = {
    val hdfsConf = sparkSession.sparkContext.hadoopConfiguration
    val fs       = FileSystem.get(new URI(targetPath), hdfsConf)
    val fsPath   = new Path(targetPath)
    if (fs.exists(fsPath))
      fs.delete(fsPath, true)
    else
      false
  }

  def getDropPartitionsSql(
      tableFullName: String,
      partitionNames: Seq[String],
      existsCheck: Boolean = false): String = {
    val dropString = if (existsCheck) {
      s"DROP IF EXISTS"
    } else {
      s"DROP"
    }
    s"""ALTER TABLE `${tableFullName
      .split("\\.")
      .mkString("`.`")}` $dropString """ + partitionNames
      .map(
        _.split("/")
          .map(token => {
            val params = token.split("=")
            params(0) + "='" + params(1) + "'"
          })
          .mkString(","))
      .map(p => s"PARTITION ($p)")
      .mkString(",\n")
  }

}
