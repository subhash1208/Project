package com.adp.datacloud.ds.nosql

import com.adp.datacloud.cli.{DxrNosqlDataExtractorConfig, dxrNosqlDataExtractorOptions}
import com.adp.datacloud.ds.DXRDataExtractor
import com.adp.datacloud.ds.rdbms.dxrDataWriter
import com.adp.datacloud.ds.util._
import com.mongodb.spark.config._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import java.io.{PrintWriter, StringWriter}

object dxrNoSqlDataExtractor extends DXRDataExtractor {

  val logger              = Logger.getLogger(getClass)
  val noSqlTableNameRegex = s"\\{noSqlTable:(.*)\\}".r

  def main(args: Array[String]): Unit = {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val config = dxrNosqlDataExtractorOptions.parse(args)
    implicit val sparkSession = SparkSession
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

    processJob(config)
  }

  def processJob(config: DxrNosqlDataExtractorConfig)(implicit
      sparkSession: SparkSession): Unit = {

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    logger.info(s"DXR_CONNECTION_STRING=${config.dxrJdbcUrl}")
    logger.info(s"DXR_SQL_STRING=${config.dxrSql}")
    logger.info(s"EXTRACT_CONFIG=${write(config)}")

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

      val connectionsDS: Dataset[DXRConnection] =
        getConnectionsDfFromConfig(config, incrementalDates)

      val connectionsMap = connectionsDS.collect().toList.groupBy(_.SQL_NAME).par

      val extractStartTime = System.currentTimeMillis()

      val failedSqls = connectionsMap.flatMap {
        case (sqlName, dxConnections) => {
          val failures =
            try {
              val dfs = dxConnections.map(dxrConnection => {
                extractData(dxrConnection, config.readOptions, dxrLogger)
              })
              logger.info(s"finished extracting $sqlName")

              val mergedDf = if (dfs.tail.nonEmpty) {
                dfs.tail.foldLeft(dfs.head)((first, next) => {
                  first.union(next)
                })
              } else {
                dfs.head
              }

              if (!mergedDf.isEmpty) {
                try {
                  writeData(config, sqlName, mergedDf, dxrLogger)
                  logger.info(s"finished writing $sqlName to table")
                  None
                } catch {
                  case e: Exception => {
                    logger.error(s"failed writing $sqlName to table", e)
                    Some(Map("failed-write" -> sqlName))
                  }
                }
              } else {
                logger.info(s"finished writing $sqlName")
                Some(Map("zero-rows" -> sqlName))
              }

            } catch {
              case e: Exception => {
                logger.error(
                  s"failed extracting data for $sqlName please revisit the query",
                  e)
                Some(Map("failed-extract" -> sqlName))
              }
            }
          failures
        }
      }

      val extractEndTime       = System.currentTimeMillis()
      val extractWallClockTime = extractEndTime - extractStartTime
      logger.info(s"DXR_EXTARCT_WALL_CLOCK_TIME_MS: $extractWallClockTime")

      val sqlFailuresMap = failedSqls.flatMap(x => x).toMap

      val connectionErrorsCount =
        sqlFailuresMap.filter(x => x._1 == "failed-extract").values.size
      val zeroCountSqls =
        sqlFailuresMap.filter(x => x._1 == "zero-rows").values.toList
      val failedWrites =
        sqlFailuresMap.filter(x => x._1 == "failed-write").values.toList

      dxrLogger.calculateAndRecordExtractSummary(
        jobStartTime        = extractStartTime,
        taskLogsDS          = None,
        landingDatabaseName = config.landingDatabaseName,
        failedWrites        = failedWrites)

      processExtractResult(
        incrementalDates      = incrementalDates,
        dxrLogger             = dxrLogger,
        strictMode            = config.strictMode,
        landingDatabaseName   = config.landingDatabaseName,
        corruptDataSqls       = List[String](),
        zeroCountSqls         = zeroCountSqls,
        failedWrites          = failedWrites,
        connectionErrorsCount = connectionErrorsCount,
        extractWallClockTime  = extractWallClockTime,
        writeWallClockTime    = 0)

    } catch {
      case de: DXR2Exception => {
        // rethrow the error to mark the job a failure
        throw de
      }
      case e: Exception => {
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        dxrLogger.updateStatusLog(
          Some(config.landingDatabaseName),
          "FAILED",
          4,
          sw.toString,
          isError = true)
        throw e
      }
    }

  }

  def getConnectionsDfFromConfig(
      config: DxrNosqlDataExtractorConfig,
      incrementalDates: Option[Dataset[DXRIncrementStatus]])(implicit
      sparkSession: SparkSession): Dataset[DXRConnection] = {

    val sqlFileNames  = config.sqlFileNames
    val sqlStringsMap = config.sqlStringsMap

    getConnectionsDfFromDxrBaseConfig(
      config,
      incrementalDates,
      sqlFileNames,
      sqlStringsMap,
      dxrSql = config.dxrSql)

  }

  def extractData(
      dxrConnection: DXRConnection,
      readOptions: Map[String, String],
      dxrLogger: DXRLogger)(implicit sparkSession: SparkSession) = {

    val sqlString = dxrConnection.FINAL_SQL

    val (replacedSql, replaceMap) = getReplacedSql(sqlString)

    val extractStartTime = System.currentTimeMillis()
    replaceMap.values.foreach(collectionName => {
      val fullReadOptions = ReadConfig(
        readOptions ++ Map(
          "database" -> dxrConnection.OWNER_ID,
          "collection" -> collectionName,
          "uri" -> dxrConnection.JDBC_URL)).asOptions

      // debug string constructed for logging only
      val readOptionsForLogging = fullReadOptions.map {
        case (name, value) => {
          if (name.equalsIgnoreCase("uri")) {
            name -> "******hidden****"
          } else
            name -> value
        }
      }

      logger.info(s"DXR_EXTARCT_SQL:$replacedSql")
      logger.info(
        s"NO_SQL_READ: extracting ${dxrConnection.SQL_NAME} for TARGET_KEY=${dxrConnection.TARGET_KEY} with config $readOptionsForLogging")

      val df = sparkSession.read
        .format("com.mongodb.spark.sql.DefaultSource")
        .options(fullReadOptions)
        .load()
      df.createOrReplaceTempView(collectionName)
    })

    val resultDF = sparkSession.sql(replacedSql)
    replaceMap.values.foreach(sparkSession.catalog.dropTempView(_))

    try {
      val count = resultDF.count()
      val taskLog = DXRTaskLog(
        APPLICATION_ID           = sparkSession.sparkContext.applicationId,
        ATTEMPT_ID               = sparkSession.sparkContext.applicationAttemptId,
        SQL_NAME                 = dxrConnection.SQL_NAME,
        TARGET_KEY               = dxrConnection.TARGET_KEY.toInt,
        DB_PARTITION             = dxrConnection.DB_SCHEMA,
        FINAL_SELECT_SQL         = dxrConnection.FINAL_SQL,
        FINAL_SESSION_SETUP_CALL = dxrConnection.FINAL_SESSION_SETUP_CALL,
        EXEC_TIME_MS             = (System.currentTimeMillis() - extractStartTime).toInt,
        MSG                      = s"",
        ROW_COUNT                = count.toInt)
      dxrLogger.writeTasksLog(taskLog)
    } catch {
      case e: Exception => {
        val taskLog = DXRTaskLog(
          APPLICATION_ID           = sparkSession.sparkContext.applicationId,
          ATTEMPT_ID               = sparkSession.sparkContext.applicationAttemptId,
          SQL_NAME                 = dxrConnection.SQL_NAME,
          TARGET_KEY               = dxrConnection.TARGET_KEY.toInt,
          DB_PARTITION             = dxrConnection.DB_SCHEMA,
          FINAL_SELECT_SQL         = dxrConnection.FINAL_SQL,
          FINAL_SESSION_SETUP_CALL = dxrConnection.FINAL_SESSION_SETUP_CALL,
          EXEC_TIME_MS             = (System.currentTimeMillis() - extractStartTime).toInt,
          MSG                      = e.getMessage,
          IS_ERROR                 = true)
        dxrLogger.writeTasksLog(taskLog)
      }
    }

    resultDF
  }

  def getReplacedSql(sqlString: String) = {

    val namesMatch = noSqlTableNameRegex.findAllMatchIn(sqlString).toList
    val replaceMap = namesMatch
      .map(name => {
        val noSqlTableNameRegex(names) = name.toString()
        (name.toString() -> names)

      })
      .toMap

    val replacedSql = replaceMap.values.foldLeft(sqlString)((sql, value) => {
      sql.replaceAll(s"\\{noSqlTable:$value\\}", value)
    })

    (replacedSql, replaceMap)

  }

  def writeData(
      config: DxrNosqlDataExtractorConfig,
      tableName: String,
      dataDF: DataFrame,
      dxrLogger: DXRLogger)(implicit sparkSession: SparkSession): Unit = {

    val optOutMasterDf = if (config.greenDatabaseName.isDefined) {
      dxrDataWriter.getOptOutMasterDf(config.optoutMaster)
    } else None

    dxrDataWriter.saveToHive(
      dataDf              = dataDF,
      outputFormat        = "parquet",
      outputPartitionSpec = config.outputPartitionSpec,
      outputDatabaseTable = tableName,
      landingDatabaseName = config.landingDatabaseName,
      blueDatabaseName    = config.blueDatabaseName,
      greenDatabaseName   = config.greenDatabaseName,
      optOutDf            = optOutMasterDf,
      dxrLogger           = Some(dxrLogger),
      isOverWrite         = true,
      isVersioned         = false)

  }

}
