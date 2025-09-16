package com.adp.datacloud.ds.annual

import com.adp.datacloud.ds.udafs.{DoubleBufferType, LongBufferType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap
import org.apache.spark.sql.functions.{approx_count_distinct, countDistinct}
import org.apache.spark.sql.types._

object testApproximations {

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0)
    println(prefix + " " + elapsed + " milliseconds")
    result
  }

  def bufferSchema =
    StructType(
      Array(
        StructField("histogram_keys", DoubleBufferType),
        StructField("histogram_values", LongBufferType) // represents occurrence count
      ))

  def newBufferSchema =
    StructType(
      ((0 to 5000).toList.map { x => StructField("bucket_" + x, DoubleType) }).toArray)

  def arrayBufferSchema = StructType(Array(StructField("test", ArrayType(IntegerType))))

  def simpleTest() {
    //val test = Array.fill(10000, 300000)({0})
    //println(test.head)

    println(
      "bufferSchema = " + UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(
        bufferSchema))
    println(
      "newBufferSchema = " + UnsafeFixedWidthAggregationMap
        .supportsAggregationBufferSchema(newBufferSchema))
    println(
      "arrayBufferSchema = " + UnsafeFixedWidthAggregationMap
        .supportsAggregationBufferSchema(arrayBufferSchema))

    val p        = Math.ceil(2.0d * Math.log(1.106d / 0.05) / Math.log(2.0d)).toInt
    val m        = 1 << p
    val numWords = m / (java.lang.Long.SIZE / 6) + 1
    println(p)
    println(m)
    println(numWords)
    println(java.lang.Integer.MIN_VALUE)
    println(java.lang.Integer.MAX_VALUE)
  }

  def main(args: Array[String]) {
    //simpleTest()
    testBM()
  }

  def testBM() {

    implicit val sparkSession = SparkSession.builder
      .appName("test")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    sc.setCheckpointDir("/tmp/tpcheckpoints")

    //val df1 = spark.read.parquet("src/test/resources/bmcinput5")
    val df1 = sparkSession.read.parquet("src/test/resources/r_dw_waf_test")

    time("Finished in")({
      //df1.agg(approxCountDistinct("ooid")).show()
      df1.agg(approx_count_distinct("pers_obj_id")).show()
    })

    time("Finished in")({
      //df1.agg(countDistinct("ooid")).show()
      df1.agg(countDistinct("pers_obj_id")).show()
    })

    // 589 grouping sets
    // 2315

  }

}
