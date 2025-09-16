package com.adp.datacloud.feature

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit}

/**
 * This class can be used to define a feature
 * dependent on a logical grouping of columns.
 *
 * @param featureName  Name of the feature being generated. For ID purposes only.
 * @param groupingColumns	 the columns on which to group
 * @param aggregationFunctions  the aggregation functions to apply
 * @param integrationFunction  (optional). integration function to tie up results of all aggregation functions into a single feature
 *
 */
class GroupFeature(
    val featureName: String,
    val groupingColumns: List[String],
    val aggregationFunctions: List[Column],
    val integrationFunction: UserDefinedFunction = null)
    extends Feature {

  /**
   * Alternate constructor for just one aggregation function
   */
  def this(
      featureName: String,
      groupingColumns: List[String],
      aggregationFunction: Column) =
    this(featureName, groupingColumns, List(aggregationFunction))

  /**
   * generate the feature as per the specification
   */
  def generate(df: DataFrame): DataFrame = {

    val dfGroup =
      if (groupingColumns.size == 0)
        df.groupBy(lit(0))
      else
        df.groupBy(groupingColumns(0), groupingColumns.tail: _*)

    val aggregations = aggregationFunctions.zipWithIndex.map { x =>
      x._1.alias("GROUPFEATURE_" + x._2)
    }
    val aggregationAliases = (0 to aggregations.length - 1).toList.map({ x =>
      col("GROUPFEATURE_" + x)
    })

    // Apply the partial aggregations - and integrate them all
    val groupResult = dfGroup
      .agg(aggregations(0), aggregations.tail: _*)
      .withColumn(
        featureName,
        if (integrationFunction != null) integrationFunction(aggregationAliases: _*)
        else aggregationAliases(0))
      .select(featureName, groupingColumns: _*)

    // Inner Join it to the original dataframe and return
    df.join(groupResult, groupingColumns)

  }

}
