package com.adp.datacloud.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers

object rangePartitionTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  import sparkSession.implicits._

  private val ordersToRepartition = Seq(
    (10, "order 1", 2000d),
    (11, "order 2", 240d),
    (12, "order 3", 232d),
    (13, "order 4", 100d),
    (14, "order 5", 11d),
    (15, "order 6", 20d),
    (16, "order 7", 390d),
    (17, "order 8", 30d),
    (18, "order 9", 99d),
    (19, "order 10", 55d),
    (20, "order 11", 129d),
    (21, "order 11", 75d),
    (22, "order 13", 173d)).toDF("id", "name", "amount")

  test("partition datasets in 3 partitions without explicit order") {
    val repartitionedOrders = ordersToRepartition
      .repartitionByRange(3, $"id")
      .mapPartitions(rows => {
        val idsInPartition =
          rows.map(row => row.getAs[Int]("id")).toSeq.sorted.mkString(",")
        Iterator(idsInPartition)
      })
      .collect()

    repartitionedOrders should have size 3
    repartitionedOrders should contain allOf ("10,11,12,13,14", "15,16,17,18", "19,20,21,22")
  }

  def main(args: Array[String]) {

    val testDF = Seq(
      ("Eren Yeager", "M", "MAR 30", 15, "170cm"),
      ("Mikasa Ackerman", "F", "FEB 30", 15, "170cm"),
      ("Armin Arlert", "M", "Nov 3", 15, "163cm"),
      ("Levi Ackerman", "M", "Dec 25", 33, "160cm"),
      ("Erwin Smith", "M", "OCT 14", 36, "188cm"),
      ("Reiner Braun", "M", "AUG 1", 17, "185cm"),
      ("Bertholt Hoover", "M", "DEC 30", 16, "192cm")).toDF(
      "name",
      "gender",
      "dob",
      "age",
      "height")

    val partitionIdx = indextest(testDF).toSeq.toDF("height", "partitionidx")

    val partitionsCount = partitionIdx.count()

    val partitionSplit = 2

    val indexedDf = testDF.join(broadcast(partitionIdx), "height")

  }

  def indextest(df: DataFrame): Array[(String, Int)] = {

    val heightsIdx: Array[(String, Int)] =
      df.select("height").collect().map(_.getAs[String](0)).distinct.zipWithIndex
    heightsIdx.foreach(println)
    heightsIdx
  }

}
