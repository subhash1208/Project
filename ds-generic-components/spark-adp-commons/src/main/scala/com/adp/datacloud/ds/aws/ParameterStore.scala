package com.adp.datacloud.ds.aws

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest

/**
 * This object may not work locally because of proxy and SSL problems. 
 */
object ParameterStore {
  
  /**
   * Retrieves a parameter by name from AWS Systems Manager Parameter Store. 
   * Returns None if either the parameter does not exist or if it cannot be read.
   */
  def getParameterByName(parameterName: String, withDecryption: Boolean = false, proxy: Option[(String, Int)] = None): Option[String] = {
    
    val region = if(Regions.getCurrentRegion() != null) {
      Regions.getCurrentRegion.getName
    } else {
      "us-east-1"
    }
    
    try {
      val endpoint = s"ssm.${region}.amazonaws.com";
      val config = new AwsClientBuilder.EndpointConfiguration(endpoint, region);
      val clientConfig = (new ClientConfiguration())
      
      proxy match {
        case Some(x) => {
          clientConfig.setProxyHost(x._1)
          clientConfig.setProxyPort(x._2)
        } case None => Unit 
      }
      
      val client = AWSSimpleSystemsManagementClientBuilder.standard()
        .withEndpointConfiguration(config)
        .withClientConfiguration(clientConfig)
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .build();
        
      
      val getParameterRequest = (new GetParameterRequest()).withName(parameterName).withWithDecryption(withDecryption);
      val getParameterResult = Option(client.getParameter(getParameterRequest))
      
      getParameterResult match {
        case Some(x) => Some(x.getParameter().getValue()) 
        case None => None
      }
    } catch {
      case e: Throwable => {
        None
      }
    }
    
  }
  
}