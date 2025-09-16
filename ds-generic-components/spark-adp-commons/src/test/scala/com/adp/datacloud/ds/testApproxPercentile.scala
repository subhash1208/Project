package com.adp.datacloud.ds

import com.adp.datacloud.ds.udafs.BenHaimTomApproximatePercentile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, col, lit}

/**
 * Interesting reads
 * https://stackoverflow.com/questions/32100973/how-to-define-and-use-a-user-defined-aggregate-function-in-spark-sql
 * https://docs.cloud.databricks.com/docs/spark/1.6/index.html#examples/Dataset%20Aggregator.html
 * https://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-dataset
 *
 */
object testApproxPercentile {

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0)
    println(prefix + " " + elapsed + " milliseconds")
    result
  }

  def main(args: Array[String]) {

    lazy val sparkSession = SparkSession.builder
      .master("local[6]")
      .appName("PercentileTest")
      .enableHiveSupport()
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    import sparkSession.implicits._

    val r      = scala.util.Random
    val mydata = for (i <- 1 to 100000) yield r.nextFloat() * 1000
    //val mydata = 1 to 200000
    val a = sc.parallelize(mydata).map(x => (x, (x % 4).toInt)).toDF("value", "group")

    a.show();

    //a.groupBy("group").agg(cl($"value").as("list")).show()

    val aggregation =
      (new BenHaimTomApproximatePercentile(Array[Int](30).toSeq, 10000))(col("value"))
    val hiveAggregation =
      callUDF(
        "percentile_approx",
        col("value"),
        lit(0.3),
        lit(10000)
      ) // Returns a column of "Array" type

    time("Running Approx percentile finished in ")({
      a.groupBy("group").agg(aggregation).show()
    });

    time("Running Hive Approx percentile finished in ")({
      a.groupBy("group").agg(hiveAggregation).show()
    });

  }

}
