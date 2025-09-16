package com.adp.datacloud.ds.rosie

import com.adp.datacloud.ds.InsightEngine
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object insightEngineTest {

  private val logger = Logger.getLogger(getClass())

  implicit val sparkSession = SparkSession
    .builder()
    .appName("Test cases for insights generation")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .master("local[*]")
    .getOrCreate()

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0)
    println(prefix + " " + elapsed + " milliseconds")
    result
  }

  def main(args: Array[String]) {
    //time("Finished in ")(managerOnlyTest())
    time("Finished in ")(clientInternalTest())
  }

  def clientInternalTest() {
    val dataFrame = sparkSession.read
      .parquet("src/test/resources/data/rosie_client_insights_input")
      .filter(
        "yr_cd is not null and qtr_cd is null and d_work_state_cd is null and d_full_tm_part_tm_cd is null")
    // Set qtr_cd null to make the computation tractable while running in local
    dataFrame.show()
    val insightEngine = new InsightEngine(
      "src/test/resources/xmls/test-client_internal_insights.xml")
    val result = insightEngine.build(dataFrame.repartition(4))()
    result.filter("insight_score_annualized_turnover_rt > 10").show(200)
  }

  @Deprecated
  def managerOnlyTest() {

    val dataFrame = sparkSession.read
      .parquet("src/test/resources/data/manager_cube")
      .filter("yr_cd is not null and qtr_cd is null")
    dataFrame.show()
    val insightEngine = new InsightEngine(
      "src/test/resources/xmls/test-manager_insights.xml")
    val result = insightEngine.build(dataFrame)()
    //result.show()
    result.filter("insight_score_annualized_turnover_rt is not null").show()
    result.printSchema()

  }

}
