package com.adp.datacloud.writers

import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object RangePartitonerTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  def main(args: Array[String]) {
    sparkSession.sql("""DROP DATABASE IF EXISTS testgreenraw CASCADE""")
    sparkSession.sql(
      """CREATE DATABASE IF NOT EXISTS testgreenraw COMMENT "Testdb" LOCATION "/tmp/spark-warehouse/testgreenraw.db" """)

    // clnt_obj_id yr_cd
    val resultDF1 = bucketizeDataframe(5, List("yr_cd"), List("qtr_cd", "mnth_cd"))
    writeToTable(resultDF1, "testgreenraw.bucket_5", List("yr_cd"), "csv")
    val resultDF2 = bucketizeDataframe(3, List("yr_cd"), List("qtr_cd", "mnth_cd"))
    writeToTable(resultDF2, "testgreenraw.bucket_3", List("yr_cd"), "csv")
    val resultDF3 = bucketizeDataframe(6, List("yr_cd"))
    writeToTable(resultDF3, "testgreenraw.bucket_6_nodist", List("yr_cd"))
    val resultDF4 = bucketizeDataframe(7)
    writeToTable(resultDF4, "testgreenraw.bucket_7_nopart")

  }

  def bucketizeDataframe(
      noOfBucket: Int,
      partitionColumns: List[String] = List(),
      buketingColumns: List[String]  = List()): DataFrame = {

    import sparkSession.implicits._
    val sc = sparkSession.sparkContext

    val df =
      sparkSession.read.parquet("src/test/resources/data/rosie_client_insights_input")
    val dfNonPartitionColumns = df.columns.filter(!partitionColumns.contains(_))
    val tableColumns          = dfNonPartitionColumns.toList ++ partitionColumns

    val dfWithPartitionIdx = if (partitionColumns.isEmpty) {
      df.withColumn("partition_name", lit("NA"))
        .withColumn("partition_idx", lit(0))
    } else {
      val customPartitionColumn: Column =
        concat_ws("/", partitionColumns.map(x => concat_ws("=", lit(s"$x"), col(x))): _*)
      val dfWithPartitionColumn =
        df.withColumn("partition_name", customPartitionColumn)
      val idxseq = dfWithPartitionColumn
        .select("partition_name")
        .distinct()
        .collect()
        .map(_.getString(0))
        .zipWithIndex
        .toSeq
      val idxDf = broadcast(
        sc.parallelize(idxseq, 2).toDF("partition_name", "partition_idx")
      ) //.withColumn("partition_idx", $"partition_idx".cast(IntegerType))
      dfWithPartitionColumn.join(idxDf, Seq("partition_name"))
    }
    val targetPartiionsCount = dfWithPartitionIdx
      .select("partition_idx")
      .distinct
      .count()
      .toInt * noOfBucket

    val dfWithSubPartitionIdx = (if (buketingColumns.isEmpty) {
                                   dfWithPartitionIdx.withColumn(
                                     "bucket_idx",
                                     ceil(rand * noOfBucket))
                                 } else {
                                   dfWithPartitionIdx.withColumn(
                                     "bucket_idx",
                                     abs(hash(buketingColumns.map(col): _*)) % noOfBucket)
                                 })
      .withColumn("sub_partition_idx", expr(s"(partition_idx*$noOfBucket)+bucket_idx"))

    val colsList = partitionColumns ++ buketingColumns ++ List(
      "partition_name",
      "partition_idx",
      "bucket_idx",
      "sub_partition_idx")
    dfWithSubPartitionIdx
      .select(colsList.head, colsList.tail: _*)
      .distinct()
      .orderBy(List("partition_idx", "sub_partition_idx").map(col): _*)
      .show(500, false)

    val targetDf = dfWithSubPartitionIdx
      .repartitionByRange(targetPartiionsCount, $"sub_partition_idx")
      .sortWithinPartitions(buketingColumns.map(col): _*)
    targetDf.select(tableColumns.head, tableColumns.tail: _*)
  }

  def writeToTable(
      df: DataFrame,
      tableName: String,
      partitionColumns: List[String] = List(),
      format: String                 = "parquet"): Unit = {
    sparkSession.sql(s"drop table if EXISTS $tableName")
    df.where("1==0")
      .write
      .format(format)
      .partitionBy(partitionColumns: _*)
      .saveAsTable(tableName)
    sparkSession.table(tableName).printSchema()
    sparkSession.sql(s"show CREATE TABLE $tableName").show(100, false)
    df.write.insertInto(tableName)
  }

}
