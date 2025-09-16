package com.adp.datacloud.ds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list}

class RandomTests {
  /*
  def main (args: Array[String]) = {


    implicit val sparkSession = SparkSession.builder
                                            .appName("Spark SQL Stats Generator")
                                            .master("local")
                                            .getOrCreate()

    sparkSession.udf.register("entropy",new Entropy)
    sparkSession.udf.register("unique_value_ratio",new UniqueValRatio)

    val df = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")
    df.createTempView("employee_monthly")

    val dftest=sparkSession.sql("select entropy(employee_guid),unique_value_ratio(employee_guid),yyyymm from employee_monthly group by yyyymm")

    dftest.show(1000,false)
  }
   */
  def main(args: Array[String]) = {

    implicit val sparkSession = SparkSession.builder
      .appName("Spark SQL Stats Validator")
      .master("local")
      .getOrCreate()

    val df = sparkSession.read.parquet("src/test/resources/column_statistics")

    val xdf = sparkSession.sql("""
                       select coalesce(id,0.00) as id, yyyymm from
                       (select 6 as id, '201901' as yyyymm
                        union all
                        select 7.00 as id, '201902' as yyyymm
                        union all
                        select 8.2 as id, '201903' as yyyymm
                        union all
                        select cast('' as double) as id, '201904' as yyyymm
                        union all
                        select cast('' as double) as id, '201905' as yyyymm
                        union all
                        select cast('' as double) as id, '201906' as yyyymm
                        union all
                        select cast('' as double) as id, '201907' as yyyymm
                        union all
                        select 10 as id, '201908' as yyyymm
                        union all
                        select cast('' as double) as id, '201909' as yyyymm
                        union all
                        select cast('' as double) as id, '201910' as yyyymm
                        union all
                        select cast('' as double) as id, '201911' as yyyymm
                        union all
                        select cast('' as double) as id, '201912' as yyyymm) x""")

    xdf.printSchema
    val xvdf = xdf
      .withColumn(
        "diff",
        collect_list(col("id")).over(Window.orderBy(col("yyyymm").desc)))
      .withColumn("Q_EFF_VALUE", dixonsQValueUDF.getQValue(col("diff")))
    xvdf.printSchema

    xvdf.show(100, false)

  }
}
