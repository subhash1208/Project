package com.adp.datacloud.ds.util

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{HashPartitioner, Partitioner}

object partitionTest {
  def main(args: Array[String]) {

    // log events to view from local history server
    val eventLogDir = s"/tmp/spark-events/"
    scala.reflect.io.Path(eventLogDir).createDirectory(true, false)

    val sparkSession = SparkSession
      .builder()
      .appName("partiton_test")
      .master("local[*]")
      .config("spark.eventLog.enabled", true)
      .config("spark.eventLog.dir", eventLogDir)
      .getOrCreate()

    import sparkSession.implicits._

    val dxrSql = s"""
          (SELECT
              b.target_key,
              b.connection_id,
              b.connection_password,
              c.product_name,
              c.product_version,
              b.owner_id,
              a.dsn,
              b.client_identifier,
              b.session_setup_call,
              b.adpr_extension
          FROM
              instances a,
              targets b,
              products c
          WHERE
              a.instance_key = b.instance_key
              AND   b.product_key = c.product_key
              AND   b.active_flag = 't'
              AND   c.product_name = 'WFN-FIT-DIRECT')
        """

    val dxrJdbcUrl =
      "jdbc:oracle:thin:@cdldftef1-scan.es.ad.adp.com:1521/cri03q_svc1"

    val dxrConnectionProperties = new Properties()
    dxrConnectionProperties.setProperty(
      "driver",
      "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver")
    dxrConnectionProperties.put("user", s"ADPI_DXR")
    dxrConnectionProperties.put("password", s"adpi")

    val connDF = sparkSession.read
      .jdbc(dxrJdbcUrl, dxrSql, dxrConnectionProperties)
      .withColumn("DSN", lower(regexp_replace($"DSN", "\\s+", "")))

    val dbIndexDF = connDF
      .select($"DSN")
      .distinct
      .map(x => x.getAs[String]("DSN"))
      .collect
      .toList
      .zipWithIndex
      .toDF("DSN", "DB_ID")
    val noOfDbs = dbIndexDF.count.toInt

    dbIndexDF.show(false)

    val mergedDF = connDF
      .join(dbIndexDF, "DSN")
      .withColumn("partition_token", concat($"DB_ID", lit("_"), $"owner_id"))

    mergedDF.show()

    val pairedRdd = mergedDF.rdd.map(row => {
      (row.getAs[String]("partition_token"), row)
    })

    val noConnectionsPerDB = 15

    val repartitioned =
      pairedRdd.partitionBy(new CustomPartitioner(noConnectionsPerDB, noOfDbs))

    val distDF = repartitioned
      .mapPartitionsWithIndex(
        (index: Int, it: Iterator[(String, Row)]) => {
          val rowList = it.toList
          val dbs =
            rowList
              .map(row => {
                row._2.getAs[Int]("DB_ID")
              })
              .distinct
              .mkString("_")
          List((index, rowList.size, dbs)).iterator
        },
        true)
      .toDF("PARTITION_ID", "ROW_COUNT", "DB_ID")

    println(repartitioned.partitions.length)
    mergedDF.groupBy("DSN", "DB_ID").count().show(false)
    distDF.sort($"PARTITION_ID").show(repartitioned.partitions.length)
    distDF.sort($"ROW_COUNT").show(repartitioned.partitions.length)
    println()

  }
}

case class TESTROW(DB: Int, SQL_NAME: String, TARGET_KEY: String)

class CustomPartitioner(subpartitions: Int, numParts: Int) extends Partitioner {
  override def numPartitions: Int = subpartitions * numParts
  override def hashCode: Int      = numPartitions

  override def getPartition(key: Any): Int = {
    val token = key.asInstanceOf[String].split(s"_")
    val partiton_id = token(0).toInt * subpartitions + (token
      .hashCode() % subpartitions).abs
    println(
      s"*********${token.toSeq}***********************************${partiton_id}*******************************************************")
    partiton_id
  }

  override def equals(other: Any): Boolean =
    other match {
      case h: HashPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }
}
