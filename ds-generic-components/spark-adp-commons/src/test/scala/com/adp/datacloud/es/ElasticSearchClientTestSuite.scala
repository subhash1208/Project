package com.adp.datacloud.es

import com.adp.datacloud.ds.aws.ParameterStore
import com.adp.datacloud.util.BaseTestWrapper
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ElasticSearchClientTestSuite extends AnyFunSuite with BaseTestWrapper {

  lazy val esClient: ElasticSearchClient = {
    lazy val esPort = ParameterStore
      .getParameterByName("__ORCHESTRATION_ES_PORT__", false)
      .getOrElse("9200")
    lazy val esProtocol = ParameterStore
      .getParameterByName("__ORCHESTRATION_ES_PROTOCOL__", false)
      .getOrElse("http")
    lazy val esDomain = ParameterStore
      .getParameterByName("__ORCHESTRATION_ES_DOMAIN__", false)
      .getOrElse("localhost")
    new ElasticSearchClient(esDomain, esPort, esProtocol)
  }

  test("Test Disable Refresh") {
    esClient.createIndex("test")
    assert(esClient.disableRefresh("test"))
    esClient.deleteIndex("test")
  }

  test("Test Enable Refresh") {
    esClient.createIndex("test")
    assert(esClient.setRefreshInterval("test", 10))
    esClient.deleteIndex("test")
  }

  test("Delete Old Indices") {
    val indices = List(
      "test-201901",
      "test-201902",
      "test-201903",
      "test-201904",
      "test-201905",
      "test-201906",
      "test",
      "test-202004")
    indices.foreach(esClient.createIndex(_))
    esClient.deleteOldIndexes("test", Some("201903"))
    assert(!esClient.indexExists("test-201901"))
    assert(esClient.indexExists("test-201903"))
    indices.foreach(esClient.deleteIndex(_))
  }

  // Exercise caution while running these..
  /*test("Test Pipeline") {
    esclient.addPolicyToPipelineSafely("datacloud_pipeline",
        "states","state",
        List("state_name", "country"))
    esclient.addPolicyToPipelineSafely("datacloud_pipeline",
        "naics","naics",
        List("industry_name", "subindustries"))
  }

  test("Test activations") {
    esclient.activatePipeline("user_lookup")
  }*/

}
