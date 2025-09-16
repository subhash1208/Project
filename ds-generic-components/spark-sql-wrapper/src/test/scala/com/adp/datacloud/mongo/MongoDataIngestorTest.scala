package com.adp.datacloud.mongo

import com.adp.datacloud.cli.mongoIngestorOptions
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MongoDataIngestorTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override def beforeAll(): Unit = {
    super.beforeAll()
    // data setup
    setUpDatabase("test")
    createTableFromResource("test", "top_sampled", "top_sampled_copy.parquet")
  }

  val sqlFilePath = getPathByResourceName("mongoIngestorInput.sql")

  val testArgs =
    s"""
       |--ingest-sql $sqlFilePath
       |--hiveconf __RO_GREEN_MAIN_DB__=test
       |--mongo-db-name datacloud_recruitment
       |--mongo-collection-name test_dsgenerics
       |--mongo-domain-name $mongoDomainAWSParam
       |--mongo-port $mongoPortAWSParam
       |--mongo-username dc_rcrmnt_user
       |""".stripMargin.trim.split("\\s+")

  // keep it lazy or failures during init wont show errors properly
  lazy val testConfig = mongoIngestorOptions.parse(testArgs)

  test("should ingest into mongoDB") {
    printSparkConfAsJson()
    mongoDataIngestor.ingest(testConfig)
  }
}
