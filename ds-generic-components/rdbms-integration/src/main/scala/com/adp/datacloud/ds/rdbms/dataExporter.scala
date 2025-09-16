package com.adp.datacloud.ds.rdbms

import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.io.Source
import scala.sys.process._

object dataExporter {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {
    
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val config = com.adp.datacloud.cli.dataExportOptions.parse(args)

    implicit val sparkSession = if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      // use local mode for testing in windows
      SparkSession.builder().appName("ParallelDataExporter").master("local[*]").getOrCreate()
    } else {
      SparkSession.builder().appName("ParallelDataExporter").enableHiveSupport().getOrCreate()
    }

    import sparkSession.implicits._

    //Read the connections file and make a parallel list
    val connectionList = Source.fromFile(config.file).getLines.toList.par
    connectionList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(config.maxParallelThreads))

    //Get the required configuration variables to be used further
    val db = config.exportDatabaseName
    val sourceTable = config.exportDatabaseTable
    val targetTable = config.targetTable
    val addStringToFile = s"========== $sourceTable export failed connections =========="

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")

    //Sql to get the schema for warehouse table
    def sql = { s"(select * from $targetTable where 1=2)" }

    //Get the first connection details in order to fetch the schema of the warehouse table
    val sampleConnection = connectionList.head
    val connvar = sampleConnection.split(',')
    val sourceDatabaseUser = connvar(1)
    val sourceDatabaseHost = connvar(2)
    val sourceDatabaseSID = connvar(3)
    val sourceDatabasePassword = connvar(4)
    val jdbcConnection = s"${config.jdbcPrefix}:${sourceDatabaseUser}/${sourceDatabasePassword}@${sourceDatabaseHost}:${config.sourceDatabasePort}/${sourceDatabaseSID}"
    val warehouseDf = sparkSession.read.jdbc(jdbcConnection, sql, prop)

    //Preparing the query to extract the data from the hive table to be exported
    val basequery = s"select * from $db.$sourceTable"
    /*    val query = if (config.filterSpec != "") {
      val basequery = s"select * from $db.$sourceTable"
      config.filterSpec.split("/").toList.foldLeft(basequery)({
        (x, y) => x + y.split("=")(0) + "=" + "\"" + y.split("=")(1) + "\"" + " and "
      })
    } else
      basequery*/
    val query = basequery
    val finalquery = if (query.endsWith(" and ")) query.dropRight(5) else query

    //Prepare hive table Dataframe
    val hiveDf = sparkSession.sql(s"$finalquery")

    //Preparing the Dataframe with similar schema like warehouse table to export.
    val colsToRemove = hiveDf.columns.map { x => x.toUpperCase() }.toSeq
    val filteredDF = warehouseDf.select(warehouseDf.columns.filter(colName => !colsToRemove.contains(colName)).map(colName => new Column(colName)): _*)
    val warehouseCols = warehouseDf.columns :+ "DB_SCHEMA"
    val finalDf = filteredDF.columns.toList.foldLeft(hiveDf)({ (df, x) => df.withColumn(x, lit(null).cast(StringType)) }).select(warehouseCols.head, warehouseCols.tail: _*)

    connectionList.foreach { conn =>
      val connvar = conn.split(',')
      val fetchPartition = connvar(0)
      val sourceDatabaseUser = connvar(1)
      val sourceDatabaseHost = connvar(2)
      val sourceDatabaseSID = connvar(3)
      val sourceDatabasePassword = connvar(4)
      val connectionString = s"${config.jdbcPrefix}:${sourceDatabaseUser}/${sourceDatabasePassword}@${sourceDatabaseHost}:${config.sourceDatabasePort}/${sourceDatabaseSID}"

      try {
        finalDf.filter($"DB_SCHEMA" === fetchPartition).drop("DB_SCHEMA").write.mode(SaveMode.Append).jdbc(connectionString, targetTable, prop)
      } catch {
        case e: Exception => {
          //get the failed connection details
          scala.reflect.io.Path("/tmp/failedExportConnections.txt").createFile().appendAll(fetchPartition + "," + sourceDatabaseUser + "," + sourceDatabaseHost + "," + sourceDatabaseSID + "," + sourceDatabasePassword + ",ErrorMessage: " + e.getMessage + "\n")
          (sparkSession.emptyDataFrame)
        }
      }
    }

    //Incase of any connection failures we are tracking all details to a file in hdfs.
    if (Files.exists(Paths.get("/tmp/failedExportConnections.txt"))) {
      Seq("sed", "-i", s"1i $addStringToFile", "/tmp/failedExportConnections.txt").!
      "hdfs dfs -appendToFile /tmp/failedExportConnections.txt /tmp/failedExportConnections.txt".!
      "hdfs dfs -chmod 777 /tmp/failedExportConnections.txt".!
      "rm /tmp/failedExportConnections.txt".!
    }
  }
}