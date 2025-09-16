package com.adp.datacloud.ds

import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers._
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, LongType}

class DeequAnalyzerTest {

  def main(args: Array[String]) = {

    implicit val sparkSession = SparkSession.builder
      .appName("Spark SQL Stats Generator")
      .master("local")
      .getOrCreate()

    val df =
      sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")
    df.filter("work_state_ not in ('ca','ga','wa','ny')")
      .groupBy("yyyymm")
      .count
      .show(100, false)
    /*
before
+------+-----+
|yyyymm|count|
+------+-----+
|201903|991  |
|201902|995  |
|201901|991  |
|201812|989  |
+------+-----+
after
+------+-----+
|yyyymm|count|
+------+-----+
|201903|714  |
|201902|720  |
|201901|711  |
|201812|703  |
+------+-----+


     */

    val partition = "yyyymm"
    val partitionvalue = df
      .select(col(partition))
      .distinct()
      .collect()
      .map(x => x.getString(0))
      .toSet
      .toArray

    val inMemStateStore = partitionvalue.map(x => (x, InMemoryStateProvider()))

    val columns = df.schema.fields.map(x => x.name -> x.dataType).toList

    val context = columns.map(x =>
      x match {

        case a
            if (a._2.isInstanceOf[DoubleType] |
              a._2.isInstanceOf[LongType]) =>
          Seq(Mean(a._1), ApproxCountDistinct(a._1))

        case b => Seq(ApproxCountDistinct(b._1))

      })

    val check =
      Check(CheckLevel.Warning, "a check").isContainedIn(partition, partitionvalue)

    val analyzers = check.requiredAnalyzers()

    val all_analyzers = context.flatten

    val analysis = Analysis(all_analyzers.toSeq ++ analyzers ++ Seq(Size()))

    val results = inMemStateStore
      .map { x => (x._1, AnalysisRunner.run(df, analysis, saveStatesWith = Some(x._2))) }
      .map(x => (x._1, successMetricsAsDataFrame(sparkSession, x._2)))
      .map(x => x._2.withColumn(partition, lit(x._1)))
      .reduce(_ union _)

    results.show(1000, false)

    //  val metrics = successMetricsAsDataFrame(sparkSession,result)

    // metrics.show(1000,false)

  }

}
