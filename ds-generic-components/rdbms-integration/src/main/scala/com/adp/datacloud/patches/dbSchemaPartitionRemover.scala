package com.adp.datacloud.patches

import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object dbSchemaPartitionRemover {

  val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

    if (args.length != 1) {
      logger.error("Comma Separated list of Database names or fully qualified table names needs to be provided as input")
      System.exit(1)
    }

    val inputList = args(0).split(",")
    implicit val sparkSession = SparkSession
      .builder()
      .appName("db_schema partition remover").enableHiveSupport().getOrCreate()

    val sc = sparkSession.sparkContext

    import scala.collection.JavaConversions._

    val hiveConf = new HiveConf(new Configuration(), classOf[HiveConf])
    val metastoreClient = new HiveMetaStoreClient(hiveConf)

    sparkSession.conf.set("spark.sql.shuffle.partitions", "60")
    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkSession.conf.set("hive.exec.max.dynamic.partitions", "5000")

    inputList.foreach(input => {

      val databaseName = input.split("\\.")(0)

      // Get list of all tables in the database (except for the excluded ones if table name is not provided)
      val tableList = if (input.split("\\.").length == 1) {
        metastoreClient.getTables(databaseName, "*").filter(x => !x.startsWith("cr") && !x.startsWith("rosie")).toList
      } else {
        List(input.split("\\.")(1))
      }

      // Iterate over each table in the DB
      tableList.foreach(tableName => {

        // Read schema for the table
        val dataFrame = sparkSession.sql(s"select * from $databaseName.$tableName")
        val isPartitioned = !metastoreClient.getTable(databaseName, tableName).getPartitionKeys.isEmpty && metastoreClient.getTable(databaseName, tableName).getPartitionKeys.exists(x => x.getName.startsWith("db_schema"))

        // Correct the table structure if db_schema or db_schema_hash column is present
        if (isPartitioned && (dataFrame.columns.contains("db_schema") || dataFrame.columns.contains("db_schema_hash"))) {

          logger.info(s"Correcting table structure for $databaseName.$tableName")

          val newPartitionSpec = metastoreClient.getTable(databaseName, tableName).getPartitionKeys.filter(x => !x.getName.startsWith("db_schema")).toList.map(_.getName).mkString(",")
          val dbSchemaColumn = metastoreClient.getTable(databaseName, tableName).getPartitionKeys.filter(x => x.getName.startsWith("db_schema")).head.getName

          val hiveWriter = HiveDataFrameWriter("parquet", if (newPartitionSpec.isEmpty) None else Some(newPartitionSpec), Some(dbSchemaColumn))
          hiveWriter.insertOverwrite(s"$databaseName.${tableName}_new", dataFrame)
          sparkSession.sql(s"drop table $databaseName.$tableName")
          sparkSession.sql(s"alter table $databaseName.${tableName}_new rename to $databaseName.$tableName")
          logger.info(s"Completed correcting table structure for $databaseName.$tableName")

        } else {
          logger.info(s"Nothing to correct for $databaseName.$tableName . Skipping...")
        }

      })

    })

  }

}