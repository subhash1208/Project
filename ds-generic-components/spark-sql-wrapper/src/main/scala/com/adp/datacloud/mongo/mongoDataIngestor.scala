package com.adp.datacloud.mongo

import com.adp.datacloud.cli.{MongoIngestorConfig, mongoIngestorOptions}
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.sql.toMongoDataFrameWriterFunctions
import org.apache.log4j.Logger

object mongoDataIngestor {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]): Unit = {
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val ingestorConfig = mongoIngestorOptions.parse(args)
    val tableName      = ingestorConfig.file.split("/").last

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(s"MONGO_${tableName}")
      .enableHiveSupport()
      .getOrCreate()

    val filesString = ingestorConfig.files
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

    ingest(ingestorConfig)

  }

  def ingest(config: MongoIngestorConfig)(implicit sparkSession: SparkSession): Unit = {

    logger.info("MONGO_DB_INPUT_SQL = " + config.sql)
    val inputDF = sparkSession.sql(config.sql)

    val mongoConfigMap = Map(
      "spark.mongodb.output.uri" -> config.getMongoDBURI,
      "spark.mongodb.output.database" -> config.dbName,
      "collection" -> config.collectionName)

    inputDF.write.options(mongoConfigMap).mode("overwrite").mongo()
  }
}
