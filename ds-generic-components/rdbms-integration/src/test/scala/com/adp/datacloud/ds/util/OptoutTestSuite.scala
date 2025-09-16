package com.adp.datacloud.ds.util
import com.adp.datacloud.ds.rdbms.dxrDataWriter
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers

// @RunWith(classOf[JUnitRunner])
class OptoutTestSuite extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  // Load Optout master df
  val optoutMasterDf = sparkSession.read.parquet("src/test/resources/optout_master")

  test(
    "Opt out logic should work when using clnt_obj_id in extract (and any optional other control columns)") {
    // Load DF containing clnt_obj_id column and some test data
    val df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/clnt_obj_id.csv")

    val optedOutDf = dxrDataWriter.getOptedOutDf(df, Some(optoutMasterDf))
    assert(
      optedOutDf.count() == 3
        && !optedOutDf
          .select("clnt_obj_id")
          .distinct()
          .collect()
          .map(_(0).toString)
          .toList
          .contains("G3D235QPME87MZ02")
    ) // An example Optedout client
  }

  test("Opt out logic should work when using ooid in extract") {
    // Load DF containing aoid column and some test data
    val df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/ooid.csv")

    val optedOutDf = dxrDataWriter.getOptedOutDf(df, Some(optoutMasterDf))
    assert(
      optedOutDf.count() == 3
        && !optedOutDf
          .select("ooid")
          .distinct()
          .collect()
          .map(_(0).toString)
          .toList
          .contains("G3D235QPME87MZ02")
    ) // An example Optedout client
  }

  test("Opt out should work when using controls instead of clients") {
    // Load DF containing clnt_obj_id column and some test data
    val df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/controls.csv")

    val optedOutDf = dxrDataWriter.getOptedOutDf(df, Some(optoutMasterDf))
    assert(
      optedOutDf.count() == 3
        && !(optedOutDf
          .select("region_code")
          .distinct()
          .collect()
          .map(_(0).toString)
          .toList
          .contains("0060")
          &&
            optedOutDf
              .select("product_code")
              .distinct()
              .collect()
              .map(_(0).toString)
              .toList
              .contains("10")
          &&
            optedOutDf
              .select("company_code")
              .distinct()
              .collect()
              .map(_(0).toString)
              .toList
              .contains("6U1"))
    ) // An example Optedout control
  }

  test(
    "Opt out should have no impact when dataframe has no fields in common with optout master") {
    // Load DF containing clnt_obj_id column and some test data
    val df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/nonoptout.csv")

    val optedOutDf = dxrDataWriter.getOptedOutDf(df, Some(optoutMasterDf))
    assert(optedOutDf.count() == df.count()) // No change in record counts
  }

  test("Opt out should have no impact when it is explicitly disabled") {
    // Load DF containing clnt_obj_id column and some test data
    val df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/clnt_obj_id.csv")

    val optedOutDf = dxrDataWriter.getOptedOutDf(df, None)
    assert(optedOutDf.count() == df.count()) // No change in record counts
  }

}
