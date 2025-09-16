package com.adp.datacloud.patches

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object backupTablesRemover {

  val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

    if (args.length != 1) {
      logger.error("Comma Separated Databases list needs to be provided as input")
      System.exit(1)
    }

    val databaseList = args(0).split(",")

    val osName = System.getProperty("os.name").toLowerCase()

    implicit val sparkSession: SparkSession =
      if (osName
        .contains("windows") || osName
        .startsWith("mac")) {
        // log events to view from local history server
        val eventLogDir = s"/tmp/spark-events/"
        scala.reflect.io
          .Path(eventLogDir)
          .createDirectory(force = true, failIfExists = false)

        // use local mode for testing in windows
        SparkSession
          .builder()
          .appName("auto_bkp table remover")
          .master("local[*]")
          .config("spark.eventLog.enabled", true)
          .config("spark.eventLog.dir", eventLogDir)
          .enableHiveSupport()
          .getOrCreate()

      } else {
        SparkSession
          .builder()
          .appName("auto_bkp table remover")
          .enableHiveSupport()
          .getOrCreate()
      }

    val sc = sparkSession.sparkContext

    import scala.collection.JavaConversions._

    val hiveConf = new HiveConf(new Configuration(), classOf[HiveConf])
    val metastoreClient = new HiveMetaStoreClient(hiveConf)

    sparkSession.conf.set("spark.sql.shuffle.partitions", "60")
    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkSession.conf.set("hive.exec.max.dynamic.partitions", "5000")

    databaseList.foreach(databaseName => {

      // Get list of all tables in the database
      val tableList = metastoreClient.getTables(databaseName, "*auto_bkp*")

      // Iterate over each table in the DB
      tableList.foreach(tableName => {

        logger.info(s"Dropping table $databaseName.$tableName")
        sparkSession.sql(s"drop table $databaseName.$tableName")

      })

    })

  }

}