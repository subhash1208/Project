package com.adp.datacloud.feature

import com.adp.datacloud.ml.WorkflowStep
import org.apache.spark.sql.DataFrame

/**
 * Subsets the dataframe to the features desired
 *
 */
abstract class FeatureFilter(val columnsToKeep: List[String] = List())
    extends WorkflowStep {

  require(
    columnsToKeep != null,
    "columnsToKeep cannot be null, use an empty list instead")

  override def needsCheckpoint = false

  def run(df: DataFrame): DataFrame = {
    df.select(columnsToKeep.head, columnsToKeep.tail: _*)
  }

}
