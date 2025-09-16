package com.adp.datacloud.ml.impute

import com.adp.datacloud.feature.GroupFeature
import com.adp.datacloud.ml.WorkflowStep
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, lit, when}

/**
 * This was expensive in terms of joins. Also forces mutable use. Use MultipleImputationBuilder instead.
 */
@deprecated(
  "Deprecated for efficiency reasons. Use MultiImputationBuilder instead",
  "1.0")
abstract class AbstractMissingValueImputer(
    spark: SparkSession,
    val groupingColumns: List[String])
    extends WorkflowStep {

  /**
   * Imputes a column with a grouping aggregation, another variant with aggregateFunction name as string
   */
  def impute(
      dataFrame: DataFrame,
      featureName: String,
      groupCols: List[String],
      aggregateFunction: String,
      optionalParams: Any*): DataFrame = {

    impute(
      dataFrame,
      featureName,
      groupCols,
      callUDF(
        aggregateFunction,
        (List(col(featureName)) ++ optionalParams.map { x => lit(x) }): _*))
  }

  /**
   * Another variant of imputation
   */
  def impute(
      dataFrame: DataFrame,
      featureName: String,
      groupCols: List[String],
      aggregateFunction: Column): DataFrame = {

    if (dataFrame.columns.contains(featureName)) {
      val imputedFeature =
        new GroupFeature(featureName + "_imputed", groupCols, aggregateFunction)

      imputedFeature
        .generate(dataFrame)
        .withColumn(
          featureName + "_new",
          when(col(featureName).isNull, col(featureName + "_imputed")).otherwise(
            col(featureName)))
        .drop(featureName)
        .drop(featureName + "_imputed")
        .withColumnRenamed(featureName + "_new", featureName)
    } else {
      dataFrame
    }

  }

}
