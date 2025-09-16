package com.adp.datacloud.ml.impute

import com.adp.datacloud.ds.udafs.BenHaimTomApproximatePercentileV2
import com.adp.datacloud.ml.WorkflowStep
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Imputes Missing Values
 */
class GenericMissingValueImputer(groupingColumns: List[String])(implicit
    sparkSession: SparkSession)
    extends WorkflowStep {

  /**
   * Impute Missing Values for predefined set of features
   */
  def run(dataFrame: DataFrame): DataFrame = {

    val nonGroupColumns = dataFrame.columns.toSet.diff(groupingColumns.toSet).toList

    val missingColumnSpecs = nonGroupColumns.map { x =>
      (sum(when(col(x).isNull, lit(1)).otherwise(lit(0))) * 100 / count(lit(1)))
        .alias(x + "_missing")
    } toList

    // A Map consisting of datatypes of each column (except the grouping columns)
    val typesMap = dataFrame.schema.fields.map { x => x.name -> x.dataType } toMap

    // A dataframe with the missing value percentages of each column added as another column
    val missingDetailsDf = dataFrame
      .groupBy(groupingColumns.head, groupingColumns.tail: _*)
      .agg(missingColumnSpecs.head, missingColumnSpecs.tail: _*)

    val numericColumns = typesMap.filter(x => x._2 != StringType).keys.toList.filter {
      x => !groupingColumns.contains(x)
    }

    val imputationBuilder =
      new MultipleImputationBuilder(sparkSession, dataFrame, groupingColumns)

    // Impute the numeric columns with Median.
    // TODO: Make this generic to support other types of imputation
    val imputedDataFrame =
      numericColumns
        .foldLeft(imputationBuilder)((builder, featureName) => {
          builder.impute(
            featureName,
            //callUDF("percentile_approx", col(featureName), lit(0.5))
            typesMap.get(featureName).get match {
              case IntegerType =>
                (new BenHaimTomApproximatePercentileV2(Array[Int](50), 3000, true))(
                  col(featureName)).getItem(0).cast(IntegerType)
              case LongType =>
                (new BenHaimTomApproximatePercentileV2(Array[Int](50), 3000, true))(
                  col(featureName)).getItem(0).cast(LongType)
              case DoubleType =>
                (new BenHaimTomApproximatePercentileV2(Array[Int](50), 3000))(
                  col(featureName)).getItem(0)
              case x =>
                round(avg(col(featureName))).cast(x) // default aggregation function
            })
        })
        .toDF(false)

    val result = nonGroupColumns.foldLeft(
      imputedDataFrame.join(missingDetailsDf, groupingColumns))((frame, y) => {
      frame
        .withColumn(
          y + "_new",
          when(
            col(y + "_missing").lt(70).and(col(y).isNull), {

              if (typesMap.get(y).get == StringType) {
                // If the column is a string
                // TODO: Can be replaced with the mode??
                lit("UNKNOWN")
              } else {
                // If the column is a numeric, replace with median
                col(y + "_imputed")
              }

            }).otherwise(col(y)))
        .drop(y)
        .drop(y + "_missing")
        .withColumnRenamed(y + "_new", y)
    })

    result.select(dataFrame.columns.head, dataFrame.columns.tail: _*)

  }

}

object testGenericMissingValueImputer {

  def main(args: Array[String]) {

    implicit val sparkSession =
      SparkSession.builder().appName("Sparkling").master("local").getOrCreate()
    lazy val sc = sparkSession.sparkContext

    val q = sparkSession.createDataFrame(
      sc.parallelize(
          (
            List(10, 29, 15, 43, 9, 12, null, 13, 16, 15, 16, 14, 16, 15),
            List[String](
              "A",
              "A",
              "A",
              "A",
              "A",
              "A",
              "A",
              "A",
              "A",
              "B",
              "A",
              "B",
              "B",
              "B"),
            List(
              "X",
              "X",
              "X",
              null,
              "Y",
              "Z",
              "Y",
              null,
              "X",
              "Y",
              "Y",
              "Z",
              "Z",
              "Y")).zipped.toList)
        .map { x => Row(x._1, x._2, x._3) },
      StructType(
        List(
          StructField("travel_distance2", IntegerType, nullable = true),
          StructField("clnt_obj_id", StringType, nullable       = true),
          StructField("testcolumn", StringType, nullable        = true))))

    val p = new GenericMissingValueImputer(List[String]("clnt_obj_id"))

    q.show()
    p.run(q).show()

  }

}
