package com.adp.datacloud.ds

import com.adp.datacloud.cli.DxrBaseConfig
import com.adp.datacloud.ds.util.{
  DXR2Exception,
  DXRConnection,
  DXRIncrementStatus,
  DXRLogger,
  DxrUtils
}
import com.adp.datacloud.ds.utils.dxrCipher
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{Dataset, SparkSession}

trait DXRDataExtractor {

  private val logger = Logger.getLogger(getClass)

  def getConnectionsDfFromDxrBaseConfig(
      config: DxrBaseConfig,
      incrementalDates: Option[Dataset[DXRIncrementStatus]],
      sqlFileNames: List[String],
      sqlStringsMap: Map[String, String],
      isolationLevel: Int = 2,
      upperBound: Long    = 200,
      lowerBound: Long    = 0,
      dxrSql: String)(implicit sparkSession: SparkSession): Dataset[DXRConnection] = {
    import sparkSession.implicits._

    val decryptUdf: UserDefinedFunction = udf((pwd: String) => {
      dxrCipher.decrypt(pwd)
    })

    val dbSchemaUdf: UserDefinedFunction = udf(
      (ownerId: String, dsn: String, connectionId: String) => {
        DxrUtils.getDbSchema(ownerId, dsn, connectionId)
      })

    // TODO: Incremental date shall be sourced from a special hiveconf "INCREMENTALDATE" parameter
    // TODO: check if PRODUCT_VERSION is being used any where and if it needs to be supported

    val rawConnectionsDF = sparkSession.read
      .jdbc(config.dxrJdbcUrl, dxrSql, config.dxrConnectionProperties)
      .withColumn(
        "TARGET_KEY",
        $"TARGET_KEY".cast(IntegerType)
      ) // convert float values to int
      .withColumn(
        "DSN",
        (config.jdbcDBType match {
          case "oracle" =>
            lower(
              regexp_replace($"DSN", "\\s+", "")
            ) // oracle is case insensitive w.r.t to DNS
          case _ => regexp_replace($"DSN", "\\s+", "")
        }))
      .withColumn("SOURCE_DB", lit(config.jdbcDBType))
      .withColumn("TRANSACTION_ISOLATION_LVL", lit(isolationLevel))
      .withColumn("DB_PASSWORD", decryptUdf($"CONNECTION_PASSWORD"))
      .drop($"CONNECTION_PASSWORD")
      .withColumn(
        "OWNER_ID",
        when($"OWNER_ID".isNull, lit("")).otherwise(trim($"OWNER_ID")))
      .withColumn(
        "CONNECTION_ID",
        when($"CONNECTION_ID".isNull, lit("")).otherwise(trim($"CONNECTION_ID")))
      .withColumn(
        "JDBC_URL",
        config.jdbcDBType match {
          case "oracle" => {
            val jdbcPrefix = s"jdbc:oracle:thin"
            when($"DSN".contains(lit(jdbcPrefix)), $"DSN").otherwise(
              concat(lit(jdbcPrefix), lit(":@"), $"DSN"))
          }
          case "sqlserver" => {
            val jdbcPrefix = s"jdbc:sqlserver"
            when(lower($"DSN").contains(lit(jdbcPrefix)), $"DSN").otherwise(
              concat(lit(jdbcPrefix), lit("://"), $"DSN"))
          }
          case "mysql" => {
            val jdbcPrefix = s"jdbc:myslq"
            when(lower($"DSN").contains(lit(jdbcPrefix)), $"DSN").otherwise(
              concat(lit(jdbcPrefix), lit("://"), $"DSN"))
          }
          case "postgres" => {
            val jdbcPrefix = s"jdbc:postgresql"
            when(lower($"DSN").contains(lit(jdbcPrefix)), $"DSN").otherwise(
              concat(lit(jdbcPrefix), lit("://"), $"DSN"))
          }
          case "redshift" => {
            val jdbcPrefix = s"jdbc:redshift"
            when(lower($"DSN").contains(lit(jdbcPrefix)), $"DSN").otherwise(
              concat(lit(jdbcPrefix), lit("://"), $"DSN"))
          }
          case "mongo" => {
            val jdbcPrefix = s"mongodb://"
            concat(
              lit(jdbcPrefix),
              $"CONNECTION_ID",
              lit(":"),
              $"DB_PASSWORD",
              lit("@"),
              $"DSN")
          }
        })
      .withColumn(
        "FINAL_SESSION_SETUP_CALL",
        when($"SESSION_SETUP_CALL".isNull, lit("")).otherwise(
          trim($"SESSION_SETUP_CALL")))
      .drop($"SESSION_SETUP_CALL")
      .withColumn(
        "ADPR_EXTENSION",
        when($"ADPR_EXTENSION".isNull, lit("")).otherwise(trim($"ADPR_EXTENSION")))
      .withColumn(
        "PRODUCT_NAME",
        when($"PRODUCT_NAME".isNull, lit("")).otherwise(trim($"PRODUCT_NAME")))
      .withColumn(
        "CLIENT_IDENTIFIER",
        when($"CLIENT_IDENTIFIER".isNull, lit("")).otherwise(trim($"CLIENT_IDENTIFIER")))
      .withColumn("SQL_NAME", explode(typedLit(sqlFileNames)))
      .withColumn("DB_SCHEMA", dbSchemaUdf($"OWNER_ID", $"DSN", $"CONNECTION_ID"))
      .withColumn(
        "FINAL_SESSION_SETUP_CALL",
        when($"ADPR_EXTENSION" === "", $"FINAL_SESSION_SETUP_CALL").otherwise(
          regexp_replace(
            $"FINAL_SESSION_SETUP_CALL",
            lit("\\[EXTENSION\\]"),
            $"ADPR_EXTENSION")))
      .withColumn(
        "FINAL_SESSION_SETUP_CALL",
        when($"CLIENT_IDENTIFIER" === "", $"FINAL_SESSION_SETUP_CALL")
          .otherwise(
            regexp_replace(
              $"FINAL_SESSION_SETUP_CALL",
              lit("\\[CLIENTID\\]"),
              $"CLIENT_IDENTIFIER")))

    // generate a unique Id against each distinct DB
    val dbIndexDF = rawConnectionsDF
      .select("DSN")
      .distinct
      .map(x => x.getAs[String]("DSN"))
      .collect
      .toList
      .zipWithIndex
      .toDF("DSN", "DB_ID")

    val connDf = rawConnectionsDF
      .join(dbIndexDF, "DSN")

    val sqlStringDf = sqlStringsMap.toList.toDF("SQL_NAME", "FINAL_SQL")

    val connectionsDF = sqlStringDf.join(connDf, "SQL_NAME")

    val substitutedDF = connectionsDF
      .withColumn(
        "FINAL_SQL",
        when($"ADPR_EXTENSION" === "", $"FINAL_SQL").otherwise(
          regexp_replace($"FINAL_SQL", lit("\\[EXTENSION\\]"), $"ADPR_EXTENSION")))
      .withColumn(
        "FINAL_SQL",
        when($"OWNER_ID" === "", $"FINAL_SQL")
          .otherwise(regexp_replace($"FINAL_SQL", lit(s"\\[OWNERID\\]"), $"OWNER_ID")))
      .withColumn(
        "FINAL_SQL",
        when($"PRODUCT_NAME" === "", $"FINAL_SQL").otherwise(
          regexp_replace($"FINAL_SQL", lit("\\[PRODUCTNAME\\]"), $"PRODUCT_NAME")))
      .withColumn(
        "FINAL_SQL",
        when($"DB_SCHEMA" === "", $"FINAL_SQL")
          .otherwise(regexp_replace($"FINAL_SQL", lit("\\[DBSCHEMA\\]"), $"DB_SCHEMA")))
      .withColumn(
        "FINAL_SQL",
        when($"CONNECTION_ID" === "", $"FINAL_SQL")
          .otherwise(
            regexp_replace($"FINAL_SQL", lit("\\[CONNECTIONID\\]"), $"CONNECTION_ID")))
      .withColumn(
        "FINAL_SQL",
        when($"CLIENT_IDENTIFIER" === "", $"FINAL_SQL")
          .otherwise(
            regexp_replace($"FINAL_SQL", lit("\\[CLIENTID\\]"), $"CLIENT_IDENTIFIER")))
      .withColumnRenamed("CONNECTION_ID", "DB_USER_NAME")
      .withColumn("ROW_ID", monotonically_increasing_id)
      .withColumn("PARTITION_KEY", lit(""))

    // check for incremental dates
    val replacedDf = if (incrementalDates.isDefined) {
      val incrementalDatesDF = incrementalDates.get.drop("APPLICATION_ID", "ENV_NAME")
      substitutedDF
        .join(incrementalDatesDF, usingColumns = Seq("SQL_NAME"), joinType = "left_outer")
        .withColumn(
          "FINAL_SQL",
          when(
            $"INCREMENTAL_END_DATE".isNotNull,
            regexp_replace(
              $"FINAL_SQL",
              lit("\\[INCREMENTALENDDATE\\]"),
              $"INCREMENTAL_END_DATE")).otherwise($"FINAL_SQL"))
        .withColumn(
          "FINAL_SQL",
          when(
            $"INCREMENTAL_START_DATE".isNotNull,
            regexp_replace(
              $"FINAL_SQL",
              lit("\\[INCREMENTALSTARTDATE\\]"),
              $"INCREMENTAL_START_DATE")).otherwise($"FINAL_SQL"))
        .withColumn(
          "FINAL_SQL",
          when(
            $"INCREMENTAL_START_DATE".isNotNull,
            regexp_replace(
              $"FINAL_SQL",
              lit("\\[INCREMENTALDATE\\]"),
              $"INCREMENTAL_START_DATE")).otherwise($"FINAL_SQL"))
        .withColumn(
          "FINAL_SESSION_SETUP_CALL",
          when(
            $"INCREMENTAL_START_DATE".isNotNull,
            regexp_replace(
              $"FINAL_SESSION_SETUP_CALL",
              lit("\\[INCREMENTALDATE\\]"),
              $"INCREMENTAL_START_DATE")).otherwise($"FINAL_SESSION_SETUP_CALL"))
    } else {
      substitutedDF
    }

    // checking optional columns in the connections df

    val providedColumns = replacedDf.columns

    // client_oid
    val withClientOidDf = if (providedColumns.contains("CLIENT_OID")) {
      replacedDf
        .withColumn(
          "CLIENT_OID",
          when($"CLIENT_OID".isNull, lit("")).otherwise(trim($"CLIENT_OID")))
        .withColumn(
          "FINAL_SQL",
          when($"CLIENT_OID" === "", $"FINAL_SQL").otherwise(
            regexp_replace($"FINAL_SQL", lit("\\[CLIENTOID\\]"), $"CLIENT_OID")))
    } else replacedDf

    // check for batching columns
    // cast decimal columns to long
    val batchCheckedDf = if (providedColumns.contains("BATCH_NUMBER")) {
      withClientOidDf.withColumn("BATCH_NUMBER", $"BATCH_NUMBER".cast(LongType))
    } else withClientOidDf.withColumn("BATCH_NUMBER", lit(0))

    val lowerBoundCheckedDf = if (providedColumns.contains("LOWER_BOUND")) {
      batchCheckedDf.withColumn("LOWER_BOUND", $"LOWER_BOUND".cast(LongType))
    } else batchCheckedDf.withColumn("LOWER_BOUND", lit(lowerBound))

    val finaldf = if (providedColumns.contains("UPPER_BOUND")) {
      lowerBoundCheckedDf.withColumn("UPPER_BOUND", $"UPPER_BOUND".cast(LongType))
    } else lowerBoundCheckedDf.withColumn("UPPER_BOUND", lit(upperBound))

    val connFinalDSWithLineage = finaldf
      .select(
        "SOURCE_DB",
        "DB_USER_NAME",
        "DB_PASSWORD",
        "TARGET_KEY",
        "OWNER_ID",
        "DB_SCHEMA",
        "JDBC_URL",
        "DB_ID",
        "SQL_NAME",
        "FINAL_SQL",
        "FINAL_SESSION_SETUP_CALL",
        "TRANSACTION_ISOLATION_LVL",
        "CLIENT_IDENTIFIER",
        "ROW_ID",
        "BATCH_NUMBER",
        "PARTITION_KEY",
        "LOWER_BOUND",
        "UPPER_BOUND")
      .as[DXRConnection]

    /**
     * removing lineage to preserve the connection details at the point when extract starts.
     * any changes to dxr catalog should not be picked up again as it might cause issues when validating the sql count for post extract.
     *
     * @see [[https://jira.service.tools-pi.com/browse/DCPL-6933]]
     */

    // Explain plan logs password values in std. comment it out when checking it in
    //    logger.info(s"logging lineage for connFinalDSWithLineage in stdout")
    //    connFinalDSWithLineage.explain()

    logger.info(s"removing lineage from connFinalDSWithLineage")
    val connFinalDSWithOutLineage = connFinalDSWithLineage.collect().toList.toDS()

    connFinalDSWithOutLineage.explain()
    connFinalDSWithOutLineage
  }

  def processExtractResult(
      dxrLogger: DXRLogger,
      incrementalDates: Option[Dataset[DXRIncrementStatus]],
      strictMode: Boolean,
      landingDatabaseName: String,
      corruptDataSqls: List[String],
      zeroCountSqls: List[String],
      failedWrites: List[String],
      connectionErrorsCount: Long,
      extractWallClockTime: Long,
      writeWallClockTime: Long): Unit = {

    // save the incremental dates
    if (incrementalDates.isDefined) {
      val incrementalDatesDS: Dataset[DXRIncrementStatus] = incrementalDates.get
      val failedSqlsList =
        (failedWrites ++ zeroCountSqls).filterNot(_.isEmpty)
      if (failedSqlsList.nonEmpty) {
        logger.info(
          s"DXR_MANAGED_INCREMENTAL: skipping record incremental dates for failures ${failedSqlsList
            .mkString(",")}")
        val filteredDatesDS: Dataset[DXRIncrementStatus] =
          incrementalDatesDS.where(
            s"SQL_NAME NOT IN ${failedSqlsList.mkString("('", "','", "')")}")
        if (!filteredDatesDS.isEmpty) {
          dxrLogger.insertIncrementalDates(filteredDatesDS, Some(landingDatabaseName))
        } else {
          logger.warn(
            s"DXR_MANAGED_INCREMENTAL: no sqls left to record incremental dates after skipping failures ${failedSqlsList
              .mkString(",")}")
        }
      } else {
        dxrLogger.insertIncrementalDates(incrementalDatesDS, Some(landingDatabaseName))
      }

    }

    val statusMsgString =
      s"EXTRACT_WALL_CLOCK_TIME_MS=$extractWallClockTime,WRITE_WALL_CLOCK_TIME_MS=$writeWallClockTime,CONNECTION_ERROR_COUNT=$connectionErrorsCount,ZERO_RECORDS_SQLS=${zeroCountSqls
        .mkString(",")},FAILED_WRITE_SQLS=${failedWrites.mkString(
        ",")},CORRUPT_DATA_SQLS=${corruptDataSqls.mkString(",")}"

    // TODO: improve logging, capture individual counts and errors a columns in delta lake table
    strictMode match {
      case true
          if connectionErrorsCount > 0 || failedWrites.nonEmpty || corruptDataSqls.nonEmpty => {
        logger.warn(s"DXR_EXECUTION_STATUS: $statusMsgString")
        dxrLogger.updateStatusLog(
          Some(landingDatabaseName),
          "FAILED",
          4,
          statusMsgString,
          isError = true)

        // log job as failure if there were errors during extracts/writes in strict mode
        throw DXR2Exception(statusMsgString)
      }
      case false if failedWrites.nonEmpty || corruptDataSqls.nonEmpty => {
        logger.warn(s"DXR_EXECUTION_STATUS: $statusMsgString")
        dxrLogger.updateStatusLog(
          Some(landingDatabaseName),
          "FAILED",
          4,
          statusMsgString,
          isError = true)
        // log job as failure if there were errors during writes in non strict mode
        throw DXR2Exception(statusMsgString)
      }
      case _ => {
        logger.info(s"DXR_EXECUTION_STATUS: $statusMsgString")
        dxrLogger.updateStatusLog(
          Some(landingDatabaseName),
          "FINISHED",
          2,
          statusMsgString)
      }
    }
  }
}
