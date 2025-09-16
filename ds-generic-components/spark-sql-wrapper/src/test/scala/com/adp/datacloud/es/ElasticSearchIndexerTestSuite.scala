package com.adp.datacloud.es

import com.adp.datacloud.cli.{ElasticSearchIndexerConfig, elasticSearchIndexerOptions}
import com.adp.datacloud.ds.aws.SecretsStore
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ElasticSearchIndexerTestSuite extends SparkTestWrapper with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("test")
  }

  val esPassword =
    SecretsStore.getCredential("__ORCHESTRATION_ES_CREDENTIALS__", esUserName).get

  override def appName: String = getClass.getCanonicalName

  override lazy implicit val sparkSession: SparkSession =
    getTestSparkSessionBuilder()
      .config("spark.es.protocol", "http")
      .config("spark.es.port", "80")
      .getOrCreate()

  val esClient = new ElasticSearchClient(
    esDomainAWSParam,
    esPortAWSParam,
    esProtocolAWSParam,
    2,
    esUserName,
    esPassword)

  test("Disable Refresh") {
    println("Password is " + esPassword)
    esClient.deleteOldIndexes("tennis", Some("9999"))
    val args =
      "--ingest-sql sample.sql --index-name tennis --auto-generate-id true --partition-column yyyymm --username ds_ingest"
        .split(" ")
    val config = elasticSearchIndexerOptions.parse(args)
    val conf = new SparkConf()
      .setMaster("local")
      .set("spark.es.index.auto.create", "true")
      .set("spark.es.nodes", esDomainAWSParam)
      .set("spark.es.port", esPortAWSParam)
      .set("spark.es.protocol", esProtocolAWSParam)
      .set("spark.es.nodes.wan.only", "true")
      .set("spark.es.net.ssl", if (esProtocolAWSParam != "https") "false" else "true")
      .set("spark.es.nodes.client.only", "false")

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .config(conf)
      .appName(s"ES_INGEST")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.addFile("src/test/resources/sample.sql")
    elasticSearchIndexer.ingest(config)
  }

  lazy val esiConfig: ElasticSearchIndexerConfig = getESIndexerConfig()

  def getESIndexerConfig(): ElasticSearchIndexerConfig = {
    val args =
      """
        |--ingest-sql sample.sql
        |--index-name tennis
        |--auto-generate-id true
        |--partition-column yyyymm
        |--hiveconf __GREEN_RAW_DB__=test
        |""".stripMargin.trim
        .split("\\s+")
    sparkSession.sparkContext.addFile("src/test/resources/sample.sql")
    val config = elasticSearchIndexerOptions.parse(args)
    config
  }

  test("get Es Config") {
    val credentialsMap = elasticSearchIndexer.getESCredentialsConfMap(esiConfig)
    assert(credentialsMap.getOrElse("spark.es.net.http.auth.user", "").nonEmpty)
    assert(credentialsMap.getOrElse("spark.es.net.http.auth.pass", "").nonEmpty)
  }

  test("try ingestion ") {
    elasticSearchIndexer.ingest(esiConfig)
  }

}
