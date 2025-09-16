package com.adp.datacloud.feature

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

/**
 * Used for defining features which require the use of
 * grouping as well as partitioning of the input dataframe
 *
 * @param featureName  Name of the feature being generated. For ID purposes only.
 * @param partitionColumns The list of column names on which to partition on.
 * @param orderColums The list of column expressions on which to order. Expressions used here instead of names to enable specifying order.
 * @param partitionFunctions The list of partition functions expressions to apply - like rank, denserank, lag, lead etc..
 * @param rankFilter An optional filter on rank criteria
 * @param groupColumns	 the columns on which to group
 * @param rankFilteredColumns the columns which should be retained while applying the rank filter. This is mandatory when rankFilter is not null
 * @param windowAggregationFunctions  the aggregation functions to apply
 * @param windowIntegrationFunction  (optional). integration function to tie up results of all aggregation functions into a single feature
 *
 */
class WindowFeature(
    val featureName: String,
    val partitionColumns: List[String],
    val orderColumns: List[Column],
    val partitionFunctions: List[Column]               = null,
    val rankFilter: (Int) => Boolean                   = null,
    val groupColumns: List[String]                     = List(),
    val rankFilteredColumns: List[String]              = null,
    val windowAggregationFunctions: List[Column]       = null,
    val windowIntegrationFunction: UserDefinedFunction = null)
    extends Feature {
  require(
    partitionColumns != null,
    "Partition Columns cannot be null. Use an empty list if you dont want to use partitions")
  require(
    !(windowAggregationFunctions != null && partitionFunctions != null),
    "Either one of Partition Function and Group Function should be provided. Not Both.")

  private val RANK_COLUMN = "rank_rank"

  /**
   * generate the feature as per the specification
   */
  def generate(df: DataFrame): DataFrame = {

    val filterCondition = udf(rankFilter)
    val spec =
      if (partitionColumns.length > 0)
        Window
          .partitionBy(partitionColumns.map({ x => col(x) }): _*)
          .orderBy(orderColumns: _*)
      else
        Window.orderBy(orderColumns: _*)

    // Apply a filter on rank, if it exists
    // TODO: There are edge cases to be fixed in this logic
    // Note: Window functions are for some reason not working in standalone mode with Spark 1.5.0.
    // In Spark notebook or on the cluster, these functions are working just fine.
    val partitioned = if (rankFilter != null) {
      rankFilteredColumns.foldRight(df.withColumn(RANK_COLUMN, rank.over(spec)))({
        (x, y) =>
          y.withColumn(
              x + "_renamed",
              when(filterCondition(col(RANK_COLUMN)), col(x)).otherwise(lit(null)))
            .withColumnRenamed(x, x + "_original")
            .withColumnRenamed(x + "_renamed", x)
      })
    } else {
      df.withColumn(RANK_COLUMN, rank.over(spec))
    }

    if (partitionFunctions != null) {
      df.withColumn(
        featureName,
        partitionFunctions.map { x => x.over(spec) }.reduce((x, y) => x + y))
    } else if (windowAggregationFunctions != null) {
      val result = (new GroupFeature(
        featureName,
        groupColumns,
        windowAggregationFunctions,
        windowIntegrationFunction)).generate(partitioned).drop(RANK_COLUMN)
      if (rankFilter != null) {
        rankFilteredColumns.foldRight(result)({ (x, y) =>
          y.drop(x).withColumnRenamed(x + "_original", x)
        })
      } else
        result
    } else {
      partitioned.withColumnRenamed(RANK_COLUMN, featureName)
    }

  }

}
