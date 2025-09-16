package com.adp.datacloud.ds

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.adp.datacloud.ds.utils.dxrHasher
object CleanData {
  def main(args: Array[String]): Unit = {

    implicit val sparkSession = SparkSession.builder().master("local").getOrCreate()

    import sparkSession.implicits._

    val hashUdf: UserDefinedFunction =
      udf[String, String]((pwd: String) => if (pwd != null) dxrHasher.hash(pwd) else null)

    val wafdf100 = sparkSession.read
      .parquet("src/test/resources/r_dw_waf_test_100")
      .withColumn("pid", hashUdf($"pers_obj_id"))
      .drop("pers_obj_id")
      .withColumn("cid", hashUdf($"clnt_obj_id"))
      .drop("clnt_obj_id")

    wafdf100.printSchema()
    wafdf100.select("pid", "cid").show(10, false)

    wafdf100
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("src/test/resources/cleandata/dataset1")

    val wafdf = sparkSession.read
      .parquet("src/test/resources/r_dw_waf_test")
      .withColumn("pid", hashUdf($"pers_obj_id"))
      .drop("pers_obj_id")
      .withColumn("cid", hashUdf($"clnt_obj_id"))
      .drop("clnt_obj_id")
      .withColumn("mid", hashUdf($"mngr_pers_obj_id"))
      .drop("mngr_pers_obj_id")
      .withColumn("ss", hashUdf($"source_system"))
      .drop("source_system")

    wafdf.printSchema()
    wafdf.select("pid", "cid", "mid", "ss").show(10, false)

    wafdf
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("src/test/resources/cleandata/dataset2")

  }
}
