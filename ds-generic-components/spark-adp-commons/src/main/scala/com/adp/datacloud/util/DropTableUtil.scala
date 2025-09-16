package com.adp.datacloud.util

import com.adp.datacloud.cli.{DropTableUtilConfig, DropTableUtilOptions}
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object DropTableUtil {

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")
    val config: DropTableUtilConfig = DropTableUtilOptions.parse(args)
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(config.applicationName)
      .enableHiveSupport()
      .getOrCreate()

    dropTables(config)
  }

  def dropTables(config: DropTableUtilConfig)(implicit sparkSession: SparkSession) {
    require(config.tableNames.nonEmpty, "table names cannot be empty")
    require(config.databases.nonEmpty, "database names cannot be empty")
    dropTablesByList(config.tableNames, config.databases)
  }

  def dropTablesByList(tableNames: List[String], databases: List[String])(implicit
      sparkSession: SparkSession): Unit = {
    val failures = databases.par
      .flatMap(dbName => {
        val failedTables: List[String] = tableNames.flatMap(tableName => {
          val tableFullName = s"${dbName}.${tableName}"
          val failedList    = HiveDataFrameWriter.dropAllTableVersions(tableFullName)
          if (failedList.isEmpty) {
            try {
              logger.info(s"dropping table $tableFullName")
              sparkSession.sql(s"DROP TABLE IF EXISTS $tableFullName")
              None
            } catch {
              case e: Exception => {
                logger.error(s"error while dropping the table $tableFullName", e)
                Some(tableFullName)
              }
            }
          } else {
            Some(failedList.mkString(","))
          }
        })
        failedTables
      })
      .toList

    require(
      failures.isEmpty,
      s"looks like there were failures while dropping the tables ${failures.mkString(",")} please address them before proceeding")
  }

}
