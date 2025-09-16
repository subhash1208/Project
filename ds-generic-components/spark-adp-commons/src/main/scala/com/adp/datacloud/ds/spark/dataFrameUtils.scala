package com.adp.datacloud.ds.spark

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{abs, broadcast, ceil, col, hash, lit, rand}

class dataFrameUtils {

  /**
   * repartitions the dataframe to buckets per partition
   *
   * @param df
   * @param buckets
   * @param partitionsList
   * @param clusterByColumns
   * @param customPartitionColumn
   * @return
   */
  def bucketizeDataframe(
      df: DataFrame,
      buckets: Int,
      partitionsList: List[String],
      clusterByColumns: List[String],
      customPartitionColumn: Some[String])(implicit
      sparkSession: SparkSession): Dataset[Row] = {

    import sparkSession.implicits._

    val customPartitionColumnName = customPartitionColumn.getOrElse({
      // TODO: build the logic to generate custom partition
      ""
    })

    val partitionIdxColName: String    = customPartitionColumnName + "_idx"
    val bucketIdxColName: String       = customPartitionColumnName + "_bucket_idx"
    val subPartitionIdxColName: String = customPartitionColumnName + "_sub_idx"

    // generate index for distinct partitions
    val dfWithPartitionIdx = if (partitionsList.size == 1) {
      df.withColumn(partitionIdxColName, lit(0))
    } else {
      val idxDf = broadcast(
        partitionsList.zipWithIndex
          .toDF(customPartitionColumnName, partitionIdxColName))
      df.join(idxDf, Seq(customPartitionColumnName))
    }

    val totalBucketsCount = partitionsList.size * buckets

    // generate bucketIdx and target subpartitionIdx
    val dfWithSubPartitionIdx = (if (clusterByColumns.isEmpty) {
                                   dfWithPartitionIdx.withColumn(
                                     bucketIdxColName,
                                     ceil(rand * buckets))
                                 } else {
                                   dfWithPartitionIdx.withColumn(
                                     bucketIdxColName,
                                     abs(hash(clusterByColumns.map(col): _*)) % buckets)
                                 })
      .withColumn(
        subPartitionIdxColName,
        (col(partitionIdxColName) * buckets) + col(bucketIdxColName))

    dfWithSubPartitionIdx
      .repartitionByRange(totalBucketsCount, col(subPartitionIdxColName))
      .sortWithinPartitions(clusterByColumns.map(col): _*)
  }
}
