package com.adp.datacloud.es

import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

import scala.util.parsing.json._
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.auth.AuthScope



class ElasticSearchClient(master: String, 
    port: String = "443", 
    protocol: String="https",
    retentionYears: Int = 2,
    username: String="none",
    password: String="none") {

  import org.apache.log4j.Logger
  private val logger = Logger.getLogger(getClass())
  
  private val DATACLOUD_PIPELINE = "datacloud_pipeline" 
  
  val credentialsProvider = new BasicCredentialsProvider();
  val credentials = new UsernamePasswordCredentials(username, password);
  credentialsProvider.setCredentials(AuthScope.ANY, credentials); // ES silently ignores any credentials when authentication is disabled

  
  def activatePipeline(pipelineName: String) = {
    getPipeline(pipelineName) match {
      case Some(pipeline) => {
        val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
        val put = new HttpPut(s"${protocol}://${master}:${port}/_all/_settings/")
        val requestEntity = new StringEntity(s"""{
          "index" : {
            "default_pipeline": "${pipelineName}"
          }
        }""", "application/json", "UTF-8")
        put.setEntity(requestEntity)
        val response = httpClient.execute(put)
        val status = response.getStatusLine.getStatusCode
        val responseText = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
        logger.warn(responseText)
        assert(status == 200)
        logger.info(s"Activated pipeline ${pipelineName}")
      }
      case None => logger.warn(s"Not activating the pipeline ${pipelineName} because it does not exist")
    }
  }
  
  
  /**
   * Creates a new Enrichment policy. Fails if one already exists.
   * Returns a policy if created. 
   */
  def createEnrichPolicy(index_name: String, 
        match_field: String,
        enrich_fields: List[String]) : String = {
    val policyName = index_name + "-policy"
    val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
    val put = new HttpPut(s"${protocol}://${master}:${port}/_enrich/policy/${policyName}")
    val requestEntity = new StringEntity(s"""{ 
        "match" : { 
          "indices" : "${index_name}",
          "match_field": "${match_field}",
          "enrich_fields": ["${enrich_fields.mkString("\",\"")}"] 
        } 
      }""", "application/json", "UTF-8")
    put.setEntity(requestEntity)
    val response = httpClient.execute(put)
    val status = response.getStatusLine.getStatusCode
    val responseText = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
    logger.warn(responseText)
    assert(status == 200)
    
    // Execute policy
    val post = new HttpPost(s"${protocol}://${master}:${port}/_enrich/policy/${policyName}/_execute")
    val executeResponse = httpClient.execute(post)
    assert(executeResponse.getStatusLine.getStatusCode == 200)
    policyName
  }
  
  def deletePolicyFromPipeline(pipelineName: String, policyName: String) = {
    // TODO: Delete policy from pipeline
    val processors = (getPipeline(DATACLOUD_PIPELINE) match {
      case Some(pipelineConfig) => {
        pipelineConfig.get("processors").get.asInstanceOf[List[Map[String, Any]]]
      }
      case None => List[Map[String,Any]]()
    }).filter(processor => {
      processor.get("enrich").isEmpty || {
        processor.get("enrich").get.asInstanceOf[Map[String, String]].get("policy_name").get != policyName
      }
    })
    
    updatePipeline(pipelineName, processors)
    
  }
  
  def deleteEnrichPolicy(policyName: String) = {
    val name = if (policyName.endsWith("-policy")) policyName else s"${policyName}-policy"
    // Delete the pipeline altogether
    val delete = new HttpDelete(s"${protocol}://${master}:${port}/_enrich/policy/${name}")
    val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
    val response = httpClient.execute(delete)
    val status = response.getStatusLine.getStatusCode
    val responseText = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
    assert(status == 200)
  }
  
  def updatePipeline(pipelineName: String, processors: List[Map[String, Any]]) = {
    // TODO: Update processors in a pipeline
    val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
    val listOfProcessorDefinitions = processors.map(processor => {
      val processorType = processor.keys.head
      val config = processor.get(processorType).get.asInstanceOf[Map[String,String]]
      s"""{
      "${processorType}" : {
          "policy_name": "${config.get("policy_name").get}",
          "field": "${config.get("field").get}",
          "target_field": "${config.get("target_field").get}",
          "max_matches": "${config.get("max_matches").get}"
      }}
      """
    })
    
    if (!listOfProcessorDefinitions.isEmpty) {
      val put = new HttpPut(s"${protocol}://${master}:${port}/_ingest/pipeline/${pipelineName}")
      val requestEntity = new StringEntity(s"""{ 
        "description" : "DataCloud Enrichment Pipeline",
        "processors" : [ ${listOfProcessorDefinitions.mkString(",")} ]
      }""", "application/json", "UTF-8")
      put.setEntity(requestEntity)
      val response = httpClient.execute(put)
      val status = response.getStatusLine.getStatusCode
      assert(status == 200)
    } else {
      // Delete the pipeline altogether
      val delete = new HttpDelete(s"${protocol}://${master}:${port}/_ingest/pipeline/${pipelineName}")
      val response = httpClient.execute(delete)
      val status = response.getStatusLine.getStatusCode
      assert(status == 200)
    }
  }
  
  /**
   * Creates a pipeline if not exists
   */
  def addPolicyToPipelineSafely(pipelineName: String, 
      index_name: String, 
      match_field: String, 
      enrich_fields: List[String]) = {
    
    val enrichPolicyName = getPolicy(index_name) match {
      case None => createEnrichPolicy(index_name, match_field, enrich_fields)
      case Some(policy_config) => {
        val fields_in_policy = policy_config.get("enrich_fields").get.asInstanceOf[List[String]]
        val needsRefresh = (match_field != policy_config.get("match_field").get.asInstanceOf[String]) ||
             !enrich_fields.toSet.diff(policy_config.get("enrich_fields").get.asInstanceOf[List[String]].toSet).isEmpty ||
             !policy_config.get("enrich_fields").get.asInstanceOf[List[String]].toSet.diff(enrich_fields.toSet).isEmpty
        if(needsRefresh) {
          deletePolicyFromPipeline(DATACLOUD_PIPELINE, policy_config.get("name").get.asInstanceOf[String])
          deleteEnrichPolicy(policy_config.get("name").get.asInstanceOf[String])
          createEnrichPolicy(index_name, match_field, enrich_fields)
        } else {
          policy_config.get("name").get.asInstanceOf[String]
        }
      }
    }
    
    val newEnrichPolicyAsList = List(Map("enrich" -> Map(
                  "policy_name" -> enrichPolicyName,
                  "field" -> match_field,
                  "target_field" -> index_name,
                  "max_matches" -> "1")))
    
    val processors = getPipeline(DATACLOUD_PIPELINE) match {
      case Some(pipelineConfig) => {
        val existingEnrichPolicies = pipelineConfig.get("processors").get.asInstanceOf[List[Map[String, Any]]]
        existingEnrichPolicies.filter(policy => policy.get("enrich").isEmpty || 
            policy.get("enrich").get.asInstanceOf[Map[String, String]].get("policy_name").get.asInstanceOf[String] != enrichPolicyName) ++ newEnrichPolicyAsList
      }
      case None => newEnrichPolicyAsList
    }
    
    updatePipeline(DATACLOUD_PIPELINE, processors)
    
  }
  
  
  /**
   * Returns a pipeline configuration if the pipeline is available
   */
  def getPipeline(pipelineName: String): Option[Map[String, Any]] = {
    try {
      val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
      val get = new HttpGet(s"${protocol}://${master}:${port}/_ingest/pipeline/${pipelineName}")
      val response = httpClient.execute(get)
      val status = response.getStatusLine.getStatusCode
      val responseText = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
      val parsed = JSON.parseFull(responseText)
      Some(parsed.get.asInstanceOf[Map[String,Any]].get(pipelineName).get.asInstanceOf[Map[String, Any]])
    } catch {
      case ex: Exception => None
    }
  }
  
  
  def getPolicy(policy: String): Option[Map[String, Any]] = {
    try {
      val policyName = if (policy.endsWith("-policy")) policy else s"${policy}-policy"
      val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
      val get = new HttpGet(s"${protocol}://${master}:${port}/_enrich/policy/${policyName}")
      val response = httpClient.execute(get)
      val status = response.getStatusLine.getStatusCode
      val responseText = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
      val parsed = JSON.parseFull(responseText).get.asInstanceOf[Map[String, Map[String, Any]]]
      Some(parsed.get("policies").get.asInstanceOf[List[Map[String, Any]]](0).
          get("config").get.asInstanceOf[Map[String,Any]].
          get("match").get.asInstanceOf[Map[String, Any]])
    } catch {
      case ex: Exception => None
    }
  }
  
  
  def getEnrichPolicies(pipelineName: String): List[String] = {
    try{
      val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
      val get = new HttpGet(s"${protocol}://${master}:${port}/_ingest/pipeline/${pipelineName}")
      val response = httpClient.execute(get)
      val status = response.getStatusLine.getStatusCode
      val responseText = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
      val parsed = JSON.parseFull(responseText).get.asInstanceOf[Map[String, Map[String, Any]]]
      val processors = parsed.get(pipelineName).get("processors").asInstanceOf[List[Any]]
      val enrichPolicies = processors.
           filter(x => x.asInstanceOf[Map[String,Any]].get("enrich").isDefined).
           map(x => x.asInstanceOf[Map[String,Any]].get("enrich").get.asInstanceOf[Map[String,String]].get("policy_name").get)
      response.close()
      enrichPolicies
    } catch {
      case ex: Exception => List[String]()
    }
  }
  
  
  def indexExists(indexName: String): Boolean = {
    val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
    val head = new HttpHead(s"${protocol}://${master}:${port}/${indexName}")
    val response = httpClient.execute(head)
    val status = response.getStatusLine.getStatusCode
    response.close()
    status == 200
  }
  
  def createIndex(indexName: String) {
    if(!indexExists(indexName)){
      val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
      val put = new HttpPut(s"${protocol}://${master}:${port}/${indexName}")
      val response = httpClient.execute(put)
      val status = response.getStatusLine.getStatusCode
      response.close()
      if (status != 200)
        throw new Exception(s"Cannot create index = ${indexName}")
    }
  }
  
  def deleteIndex(indexName: String) {
    if(indexExists(indexName)){
      val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
      val delete = new HttpDelete(s"${protocol}://${master}:${port}/${indexName}")
      val response = httpClient.execute(delete)
      val status = response.getStatusLine.getStatusCode
      response.close()
      if (status != 200)
        throw new Exception(s"Cannot create index = ${indexName}")
    }
  }
  
  /**
   * Deletes older indexes from ElasticSearch
   */
  def deleteOldIndexes(indexPrefix: String, olderThan: Option[String] = None): Boolean = {
    val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
    val get = new HttpGet(s"${protocol}://${master}:${port}/${indexPrefix}*")
    println(s" *****  ${protocol}://${master}:${port}/${indexPrefix}*")
    try {
      val response = httpClient.execute(get)
      val status = response.getStatusLine.getStatusCode
      val responseText = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
      response.close()
      if (status != 200) {
        logger.warn(s"Not OK response from ElasticSearch while pulling list of historical indices - ${status}")
        false
      } else {
        val parsed = JSON.parseFull(responseText).get.asInstanceOf[Map[String, Any]]
        val indices = parsed.keys.toList
        if(!indices.isEmpty){
          val latestIndex = indices.max
          if(latestIndex.split(indexPrefix+"-").size == 2) {
            val latestPartition = latestIndex.split(indexPrefix+"-")(1) 
            val defaultOlderThan = (latestPartition.take(4).toInt - retentionYears).toString + latestPartition.drop(4) 
            // Compute the indices eligible for dropping
            val indicesToDrop = indices.filter({x => {
              val splits = x.split(indexPrefix+"-")
              splits.size == 2 && splits(1) < (olderThan.getOrElse(defaultOlderThan))
              }
            })
            indicesToDrop.foreach(deleteIndex(_))
          }
        }
        true
      }
    } catch {
      case ex: Exception => {
        logger.warn("Cannot delete old indexes, but proceeding... ", ex)
        false
      }
    }
  }
  
  
  /**
   * Disables ElasticSearch index refresh
   */
  def disableRefresh(indexName: String): Boolean = {
    if(indexExists(indexName)){
      val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
      val put = new HttpPut(s"${protocol}://${master}:${port}/${indexName}/_settings")
      val requestEntity = new StringEntity("""{ "index" : {"refresh_interval" : "-1" } }""", "application/json", "UTF-8")
      try {
        put.setEntity(requestEntity)
        val response = httpClient.execute(put)
        val status = response.getStatusLine.getStatusCode
        response.close()
        if (status != 200) {
          logger.warn(s"Not OK response from ElasticSearch while disabling index refresh - ${status}")
          false
        } else {
          true
        }
      } catch {
        case ex: Exception => {
          logger.warn("Cannot disable Index Refresh Interval, but proceeding... ", ex)
          false
        }
      }
    } else {
      logger.warn("Index not found for disabling refresh, but proceeding... ")
      false
    }
  }

  
  /**
   * Sets ElasticSearch index refresh interval
   */
  def setRefreshInterval(indexName: String, refreshInterval: Integer): Boolean = {
    val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
    val put = new HttpPut(s"${protocol}://${master}:${port}/${indexName}/_settings")
    val requestEntity = new StringEntity(s"""{ "index" : { "refresh_interval" : "${refreshInterval}s" } }""", "application/json", "UTF-8")
    try {
      put.setEntity(requestEntity)
      val response = httpClient.execute(put)
      val status = response.getStatusLine.getStatusCode
      response.close()
      if (status != 200) {
        logger.warn(s"Not OK response from ElasticSearch while enabling index refresh - ${status}")
        false
      } else
        true
    } catch {
      case ex: Exception => {
        logger.warn("Cannot set Index Refresh Interval, but proceeding... ", ex)
        false
      }
    }
  }

}