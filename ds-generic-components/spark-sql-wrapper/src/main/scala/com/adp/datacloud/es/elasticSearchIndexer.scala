package com.adp.datacloud.es

import com.adp.datacloud.cli.{ElasticSearchIndexerConfig, elasticSearchIndexerOptions}
import com.adp.datacloud.ds.aws.SecretsStore
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.elasticsearch.spark.sql._

object elasticSearchIndexer {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")
    val config    = elasticSearchIndexerOptions.parse(args)
    val tableName = config.file.split("/").last

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(s"ES_INGEST_${tableName}")
      .config("es.index.auto.create", true)
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

    ingest(config)
  }

  def getESCredentialsConfMap(config: ElasticSearchIndexerConfig)(implicit
      sparkSession: SparkSession): Map[String, String] = {

    val esUserName = if (config.username.isDefined) {
      config.username.get
    } else if (sparkSession.conf.getOption("spark.es.net.http.auth.user").isDefined) {
      sparkSession.conf.get("spark.es.net.http.auth.user")
    } else ""

    if (esUserName.nonEmpty) {
      val esPassword = config.pwd match {
        case Some(y) => y
        case None =>
          logger.info(s"ES_CREDS: fetching credentials for esUserName $esUserName")
          SecretsStore.getCredential("__ORCHESTRATION_ES_CREDENTIALS__", esUserName).get
      }
      Map(
        "spark.es.net.http.auth.user" -> esUserName,
        "es.net.http.auth.user" -> esUserName,
        "spark.es.net.http.auth.pass" -> esPassword,
        "es.net.http.auth.pass" -> esPassword)
    } else {
      throw new Exception("Invalid Credentials Configuration")
    }
  }

  def getEsClient(
      dataRetentionYears: Int,
      sparkConf: RuntimeConfig,
      ESCredentialsConfMap: Map[String, String])(implicit
      sparkSession: SparkSession): ElasticSearchClient = {
    new ElasticSearchClient(
      sparkConf.get("spark.es.nodes"),
      sparkConf.get("spark.es.port", "443"),
      sparkConf.get("spark.es.protocol", "https"),
      dataRetentionYears,
      ESCredentialsConfMap.getOrElse("spark.es.net.http.auth.user", "none"),
      ESCredentialsConfMap.getOrElse("spark.es.net.http.auth.pass", "none"))
  }

  /**
   * Ingests data into a hive table in RAW database first and then into ElasticSearch
   */
  def ingest(config: ElasticSearchIndexerConfig)(implicit
      sparkSession: SparkSession): Unit = {

    if (!config.useForEnrichment && config.disablePipeline) {
      sparkSession.conf.set("es.ingest.pipeline", "datacloud_default")
    }

    logger.info("ES_INPUT_SQL = " + config.sql)
    val df = sparkSession.sql(config.sql)

    if (!config.autogenerateId && !df.columns.contains(config.mappingId))
      throw (new Exception(
        s"Data doesn't have the ${config.mappingId} column and ID auto generation is disabled"))

    if (config.indexPartitionColumn.isDefined && config.useForEnrichment)
      throw (new Exception(s"Can't use a partitioned index for enrichment. Aborting.."))

    val esCredentialsMap = getESCredentialsConfMap(config)

    // Drop any historical indices which are no longer allowed per retention policy.
    val elasticClient =
      getEsClient(config.dataRetentionYears, sparkSession.conf, esCredentialsMap)
    elasticClient.deleteOldIndexes(config.indexName)

    // take the latest run time config and update
    val esConfigurationMapping =
      if (config.autogenerateId) esCredentialsMap
      else
        esCredentialsMap
          .+("es.mapping.id" -> config.mappingId)

    config.indexPartitionColumn match {
      case Some(column) => {
        // Validation
        if (!df.columns.contains(column))
          throw (new Exception("Partition Column not found in data being ingested"))

        // Write the same content to Hive also for backup purposes.
        val rawDbName = config.getParam("__GREEN_RAW_DB__")
        rawDbName match {
          case Some(x) => {
            val hiveWriter = HiveDataFrameWriter("parquet", Some(column))
            hiveWriter.insertSafely(
              s"${x}.es_${config.indexName.replace("-", "_")}",
              df,
              true)
          }
          case None => logger.warn("RAW DB not found. Not Saving the data to hive.")
        }

        // Write to multiple indices as appropriate for this dataframe
        val partitions =
          df.select(column).distinct().collect().map(x => x.getAs[String](0))
        partitions.foreach(partitionValue => {

          val indexName = s"${config.indexName}-${partitionValue}"

          // Disable Index Refresh
          elasticClient.disableRefresh(indexName)

          // Write to Index
          df.filter(col(column).equalTo(lit(partitionValue)))
            .repartition(config.parallelism)
            .saveToEs(s"${indexName}", esConfigurationMapping)

          // Re-enable Index Refresh
          elasticClient.setRefreshInterval(indexName, config.indexRefreshInterval)
        })

      }

      case None => {
        // Write the same content to Hive also for backup purposes.
        val rawDbName = config.getParam("__GREEN_RAW_DB__")
        rawDbName match {
          case Some(x) => {
            val hiveWriter = HiveDataFrameWriter("parquet", None)
            hiveWriter.insertSafely(
              s"${x}.es_${config.indexName.replace("-", "_")}",
              df,
              true)
          }
          case None => logger.warn("RAW DB not found. Not Saving the data to hive.")
        }

        // Disable Index Refresh
        elasticClient.disableRefresh(config.indexName)

        // Overwrite the index as a whole
        df.repartition(config.parallelism)
          .saveToEs(s"${config.indexName}", esConfigurationMapping)

        // Re-enable Index Refresh
        elasticClient.setRefreshInterval(config.indexName, config.indexRefreshInterval)

        // Enable pipeline for enrichment if applicable
        if (config.useForEnrichment) {
          elasticClient.addPolicyToPipelineSafely(
            "datacloud_pipeline",
            config.indexName,
            config.mappingId,
            df.columns.filter(x => x != config.indexName).toList)

          // elasticClient.activatePipeline("datacloud_pipeline") // We will not activate default pipeline for now.
        }

      }
    }

  }

}
