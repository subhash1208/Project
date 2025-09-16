package com.adp.datacloud.feature

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Base trait for Features
 */
trait Feature {
  def generate(df: DataFrame): DataFrame
}

class FeatureBuilder(private val dataFrame: DataFrame) {

  /**
   * Applies the feature on the dataframe and returns it
   */
  def apply(feature: Feature) = new FeatureBuilder(feature.generate(dataFrame))

  /**
   * Utility method to apply on multiple dataframes in a row.
   */
  def apply(features: List[Feature]) =
    new FeatureBuilder(
      features.foldRight(dataFrame)((feature, df) => feature.generate(df)))

  def toDF() = dataFrame

  def checkpoint(partitionExprs: Column*)(implicit sparkSession: SparkSession) = {
    dataFrame.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    dataFrame.rdd.checkpoint()
    dataFrame.take(1) // Force checkpoint
    new FeatureBuilder(
      sparkSession
        .createDataFrame(dataFrame.rdd, dataFrame.schema)
        .repartition(partitionExprs: _*))
  }

}
