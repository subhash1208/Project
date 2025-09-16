package com.adp.datacloud.ml.impute

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, lit, when}

/**
 * This class can perform imputation of multiple columns in a dataframe
 * depending on a group specification
 */
class MultipleImputationBuilder(
    spark: SparkSession,
    dataFrame: DataFrame,
    groupingColumns: List[String],
    imputations: List[Tuple2[String, Column]] = List()) {

  /**
   * Imputes a column with a grouping aggregation, another variant with aggregateFunction name as string
   */
  def impute(featureName: String, aggregateFunction: String, optionalParams: Any*) = {
    new MultipleImputationBuilder(
      spark,
      dataFrame,
      groupingColumns,
      imputations ++ List(
        (
          featureName,
          callUDF(
            aggregateFunction,
            (List(col(featureName)) ++ optionalParams.map { x => lit(x) }): _*))))
  }

  /**
   * Another variant of imputation
   */
  def impute(featureName: String, aggregateFunction: Column) = {

    new MultipleImputationBuilder(
      spark,
      dataFrame,
      groupingColumns,
      imputations ++ List((featureName, aggregateFunction)))

  }

  /**
   * Build a grouped df and apply all aggregations collected so far.
   * Each feature is replaced inplace with its imputed values if the parameter inplaceReplace is set.
   * Else new columns with suffix "_imputed" are returned.
   */
  def toDF(inplaceReplace: Boolean = false) = {
    val dfGroup =
      if (groupingColumns.size == 0)
        dataFrame.groupBy(lit(0))
      else
        dataFrame.groupBy(groupingColumns(0), groupingColumns.tail: _*)

    val aggregations = imputations.map({
      case (featureName, aggregateFunction) =>
        aggregateFunction.alias(featureName + "_imputed")
    })

    val dfWithImputedFeatures = dataFrame.join(
      dfGroup.agg(aggregations.head, aggregations.tail: _*),
      groupingColumns)

    val featureNames = imputations.map(x => x._1)

    if (!inplaceReplace) {
      dfWithImputedFeatures
    } else {
      featureNames.foldRight(dfWithImputedFeatures)(
        (featureName, df) =>
          df.withColumn(
              featureName + "_new",
              when(col(featureName).isNull, col(featureName + "_imputed")).otherwise(
                col(featureName)))
            .drop(featureName)
            .drop(featureName + "_imputed")
            .withColumnRenamed(featureName + "_new", featureName))
    }

  }

}
