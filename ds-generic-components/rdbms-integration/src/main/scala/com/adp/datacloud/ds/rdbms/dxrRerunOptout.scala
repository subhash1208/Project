package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.dxrParallelDataExtractorOptions
import com.adp.datacloud.ds.util.{DXR2Exception, DXRLogger}
import com.adp.datacloud.writers.{DataCloudDataFrameWriterLog, HiveDataFrameWriter}
import oracle.jdbc.OracleConnection
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.{PrintWriter, StringWriter}
import java.util.Properties
import scala.collection.parallel.ForkJoinTaskSupport

object dxrRerunOptout {

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val config = dxrParallelDataExtractorOptions.parse(args)

    val dxrConnectionProperties = new Properties()
    dxrConnectionProperties.setProperty(
      "driver",
      "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver")
    // pass the the jdbc url including user name and password if wallet is not setup for local mode
    // jdbc:oracle:thin:<UserName>/<password>@<hostname>:<port>/<serviceID>
    dxrConnectionProperties.setProperty(
      OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION,
      config.oracleWalletLocation)

    Logger
      .getLogger(
        "org.apache.spark.sql.execution.datasources.parquet.CatalystWriteSupport")
      .setLevel(Level.WARN)

    implicit val sparkSession =
      if (System
          .getProperty("os.name")
          .toLowerCase()
          .contains("windows") || System
          .getProperty("os.name")
          .toLowerCase()
          .startsWith("mac")) {
        // log events to view from local history server
        val eventLogDir = s"/tmp/spark-events/"
        scala.reflect.io.Path(eventLogDir).createDirectory(true, false)

        // use local mode for testing in windows
        SparkSession
          .builder()
          .appName(config.applicationName)
          .master("local[*]")
          .config("spark.eventLog.enabled", true)
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

    val sc = sparkSession.sparkContext

    val dxrLogger = new DXRLogger(
      url                  = config.dxrJdbcUrl,
      props                = dxrConnectionProperties,
      applicationName      = config.applicationName,
      productName          = null,
      dxrSql               = null,
      inputSqls            = List[String](),
      jobConfigJson        = null,
      envName              = config.inputParams.getOrElse("env", ""),
      esCredentialsConfMap = config.getESCredentialsConfMap(sparkSession.conf.getAll),
      parentApplicationId  = sc.applicationId)

    val shufflePartitons = sparkSession.conf.get("spark.sql.shuffle.partitions").toInt

    val optoutDf = config.optoutMaster match {
      case Some(x) =>
        Some(sparkSession.sql("select * from " + config.greenDatabaseName + "." + x))
      case None => None
    }

    // Setup the writer
    val hiveWriter = HiveDataFrameWriter("parquet", config.outputPartitionSpec)

    // Read list of Tables from command line

    val listOftables = config.sqlFileNames

    val listOftablesParllelised = listOftables.par
    listOftablesParllelised.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(2))

    // For each table call Insert into Green and Insert into blue.

    val writeFailures = listOftablesParllelised
      .flatMap { tableName =>
        try {
          logger.info(s"Started for Table $tableName")

          val sql = config.optOutRerunSpec match {
            case Some(x) =>
              "SELECT * FROM " + config.landingDatabaseName + "." + tableName + " WHERE " + x
            case None => "SELECT * FROM " + config.landingDatabaseName + "." + tableName
          }

          val landingDbDF = sparkSession.sql(sql)

          //Write to Blue DB
          val optedOutDf = dxrDataWriter.saveToBlueDB(
            config.blueDatabaseName,
            landingDbDF,
            optoutDf,
            tableName,
            Some(dxrLogger),
            outputFormat        = config.outputFormat,
            outputPartitionSpec = config.outputPartitionSpec,
            isOverWrite         = true,
            isVersioned         = config.isVersioned)

          // Write to Green DB
          dxrDataWriter.saveToGreenDB(
            config.greenDatabaseName,
            optedOutDf,
            tableName,
            Some(dxrLogger),
            outputFormat        = config.outputFormat,
            outputPartitionSpec = config.outputPartitionSpec,
            isOverWrite         = true,
            isVersioned         = config.isVersioned)

          logger.info(s"All the writes for Table $tableName Finished")
          None

        } catch {
          case e: Exception => {
            logger.error(s"failed writing the table $tableName to hive: ", e)
            val sw = new StringWriter
            e.printStackTrace(new PrintWriter(sw))
            dxrLogger.insertHiveDfWriterLog(
              DataCloudDataFrameWriterLog(
                sparkSession.sparkContext.applicationId,
                sparkSession.sparkContext.applicationAttemptId,
                tableName,
                0,
                0,
                0,
                0,
                sw.toString,
                true))
            Some(tableName)
          }
        }

      }
      .toList
      .mkString(",")

    if (writeFailures.nonEmpty) {
      throw DXR2Exception(s"FAILED_WRITE_SQLS=$writeFailures")
    }

  }
}
