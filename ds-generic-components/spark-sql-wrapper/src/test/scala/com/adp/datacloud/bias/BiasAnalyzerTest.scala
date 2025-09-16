package com.adp.datacloud.bias

import com.adp.datacloud.cli.biasAnalyzerOptions
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class BiasAnalyzerTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  val logger = Logger.getLogger(getClass)

  override def beforeAll(): Unit = {
    // Data setup
    //    cleanAndSetupDatabase("test")
    setUpDatabase("test")
    createTableFromResource("test", "top_sampled", "top_sampled_copy.parquet")
    displayAllTables("test")
  }

  // Uncomment the below line to use local ES for testing
//  override val useLocalES = true

  val sqlFilePath = getPathByResourceName("biasSampleInput.sql")

  val testArgs =
    s"""
       |--input-file-path $sqlFilePath
       |--bias-check-column gndr_dsc
       |--bias-check-column martl_stus_dsc
       |--group-by-column l2_code
       |--frequency Q
       |--es-username ds_ingest
       |--hiveconf __GREEN_MAIN_DB__=test
       |""".stripMargin.trim.split("\\s+")

  // keep it lazy or failures during init wont show errors properly
  lazy val testConfig = biasAnalyzerOptions.parse(testArgs)

  printSparkConfAsJson()

  // TODO: figure out a way to assert error
  ignore("fail on invalid arguments") {
    val inputArgs =
      s"""
         |--input-file-path $sqlFilePath
         |--bias-check-column gndr_dsc
         |--bias-check-column martl_stus_dsc
         |--bias-check-column l2_code
         |--group-by-column l2_code
         |--frequency Q
         |--hiveconf __RO_GREEN_MAIN_DB__=test
         |""".stripMargin.trim.split("\\s+")

    try {
      biasAnalyzerOptions.parse(inputArgs)
    } catch {
      case e: Error => {
        assert(e.getMessage.contains("l2_code"))
      }
      case NonFatal(ex)    => logger.error("test config parse failed", ex)
      case exec: Exception => logger.error("test config parse failed", exec)
    }
  }

  test("fail on invalid bias-check-column") {
    val inputArgs =
      s"""
         |--input-file-path $sqlFilePath
         |--bias-check-column gndr_dsc
         |--bias-check-column martl_stus_dsc
         |--bias-check-column dummy
         |--group-by-column l2_code
         |--frequency Q
         |--hiveconf __GREEN_MAIN_DB__=test
         |""".stripMargin.trim.split("\\s+")

    val exception: IllegalArgumentException = intercept[IllegalArgumentException] {
      biasAnalyzer.computeMetrics(biasAnalyzerOptions.parse(inputArgs))
    }
    assert(exception.getMessage.contains("dummy column is not present in the input data"))
  }

  test("Compute Bias Metrics and write to Hive & ES Index") {
    biasAnalyzer.computeMetrics(testConfig)
  }

  test("Get Latest Version") {
    println(biasAnalyzer.getLatestVersion("test", "bias_analysis_top", "2020Q3"))
  }

}
