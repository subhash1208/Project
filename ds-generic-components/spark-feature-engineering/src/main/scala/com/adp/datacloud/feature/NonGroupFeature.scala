package com.adp.datacloud.feature

import org.apache.spark.sql.{Column, DataFrame}

/**
 * Generates a feature based on a column expression
 */
class NonGroupFeature(val featureName: String, val function: Column) extends Feature {

  /**
   * generate the feature as per the specification
   */
  def generate(df: DataFrame): DataFrame = df.withColumn(featureName, function)

}
