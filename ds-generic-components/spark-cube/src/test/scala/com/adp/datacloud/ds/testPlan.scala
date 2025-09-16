package com.adp.datacloud.ds

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lit, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object testPlan {

  def main(args: Array[String]) {

    implicit val sparkSession = SparkSession.builder
      .appName("CubeTesting")
      .master("local")
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    sc.setCheckpointDir("/tmp/tpcheckpoints")

    def time[A](prefix: String)(a: => A) = {
      val t0      = System.currentTimeMillis()
      val result  = a
      val elapsed = (System.currentTimeMillis() - t0)
      println(prefix + " " + elapsed + " milliseconds")
      result
    }

    val q = sparkSession.createDataFrame(
      sc.parallelize(
          (
            (
              List(10, 29, 15, 43, 9, 12, null, 13, 16, 15, 16, null, 16, 15),
              List(10, 77, 82, 31, 50, 70, 71, 92, 88, 80, 56, 78, 99, 74),
              List[String](
                "X",
                "X",
                "X",
                "X",
                "Y",
                "Y",
                "Y",
                "X",
                "X",
                "X",
                "X",
                "Y",
                "Y",
                "Y")).zipped.toList,
            (
              List[String](
                "X1",
                "X1",
                "X2",
                "X1",
                "X1",
                "X3",
                "X3",
                "X3",
                "X1",
                "X1",
                "X1",
                "X2",
                "X2",
                "X1"),
              List[String](
                "AK",
                "AK",
                "TN",
                "TX",
                "TX",
                "TS",
                "TS",
                "CA",
                "TN",
                "AK",
                "AK",
                "TS",
                "NJ",
                "TX")).zipped.toList).zipped.toList.map(x =>
            (x._1._1, x._1._2, x._1._3, x._2._1, x._2._2, x._2)))
        .map { x => Row(x._1, x._2, x._3, x._4, x._5) },
      StructType(
        List(
          StructField("earnings", IntegerType, nullable    = true),
          StructField("onet_score", IntegerType, nullable  = true),
          StructField("reg_temp_dsc", StringType, nullable = true),
          StructField("onet_code", StringType, nullable    = true),
          StructField("state", StringType, nullable        = true))))

    println("Original Data")
    q.show()

    time("Default cube function processing finished in ")({

      // Modify the onet_code dimension to be valid only when score > 70.
      val modifiedDF = q.withColumn(
        "onet_code_modified",
        when(col("onet_score").gt(lit(70)), col("onet_code"))
          .otherwise(lit("99-9999.00")))

      // Run a collect to force the aggregation computation
      modifiedDF
        .cube("reg_temp_dsc", "onet_code_modified", "state")
        .agg(avg("earnings"))
        .collect()

    });

    // Default cube function processing finished in  2037 milliseconds

    time("Combos unions finished in ")({

      // The combos below are hard-coded for easy explanation, but it is possible to generate these dynamically too.

      // Combos containing onet_code dimension
      // Aggregations for these combos are run on the filtered dataframe
      val filteredDf = q.filter(col("onet_score").gt(lit(70)))
      val gd1 = filteredDf
        .groupBy(col("reg_temp_dsc"), col("onet_code"), col("state"))
        .agg(avg("earnings"))
      val gd2 = filteredDf
        .groupBy(lit(null: String).as("reg_temp_dsc"), col("onet_code"), col("state"))
        .agg(avg("earnings"))
      val gd3 = filteredDf
        .groupBy(col("reg_temp_dsc"), col("onet_code"), lit(null: String).as("state"))
        .agg(avg("earnings"))
      val gd4 = filteredDf
        .groupBy(
          lit(null: String).as("reg_temp_dsc"),
          col("onet_code"),
          lit(null: String).as("state"))
        .agg(avg("earnings"))

      // Combos NOT containing onet_code dimension
      // Aggregations for these combos are run on the original dataframe
      val gd5 = q
        .groupBy(col("reg_temp_dsc"), lit(null: String).as("onet_code"), col("state"))
        .agg(avg("earnings"))
      val gd6 = q
        .groupBy(
          lit(null: String).as("reg_temp_dsc"),
          lit(null: String).as("onet_code"),
          col("state"))
        .agg(avg("earnings"))
      val gd7 = q
        .groupBy(
          col("reg_temp_dsc"),
          lit(null: String).as("onet_code"),
          lit(null: String).as("state"))
        .agg(avg("earnings"))
      val gd8 = q
        .groupBy(
          lit(null: String).as("reg_temp_dsc"),
          lit(null: String).as("onet_code"),
          lit(null: String).as("state"))
        .agg(avg("earnings"))

      // Run a collect at the end to force the aggregation computation
      gd1
        .union(gd2)
        .union(gd3)
        .union(gd4)
        .union(gd5)
        .union(gd6)
        .union(gd7)
        .union(gd8)
        .collect()

    });

    // Union based combos finished in  5117 milliseconds

  }

}
