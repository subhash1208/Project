package com.adp.datacloud.ds.aws

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, ResourceNotFoundException}
import org.apache.log4j.Logger
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object SecretsStore {

  private val logger = Logger.getLogger(getClass())
  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  /**
   * Retrieves a secret by name from AWS Secrets manager.
   * Returns None if either the secret does not exist or if it cannot be read.
   */
  def getSecretByName(secretName: String): Option[String] = {

    val region = if (Regions.getCurrentRegion() != null) {
      Regions.getCurrentRegion.getName
    } else {
      "us-east-1"
    }

    try {
      val endpoint = s"secretsmanager.${region}.amazonaws.com"
      val config = new AwsClientBuilder.EndpointConfiguration(endpoint, region)

      val clientBuilder = AWSSecretsManagerClientBuilder.standard()
      clientBuilder.setEndpointConfiguration(config)
      val client = clientBuilder.build()

      val getSecretValueRequest = (new GetSecretValueRequest()).withSecretId(secretName).withVersionStage("AWSCURRENT")
      val getSecretValueResult = Option(client.getSecretValue(getSecretValueRequest))

      getSecretValueResult match {
        case Some(x) => Some(x.getSecretString())
        case None => None
      }
    } catch {
      case re: ResourceNotFoundException => {
        logger.warn(s"Secret $secretName was not found")
        None
      }
      case t: Throwable => {
        logger.warn(s"error while fetching the Secret $secretName", t)
        None
      }
    }

  }

  /**
   * parses the secret json into Map[String, String]
   *
   * @param secretName
   * @return
   */
  def getSecretMapByName(secretName: String): Option[Map[String, Any]] = {
    val secret: Option[String] = getSecretByName(secretName)
    try {
      secret match {
        case Some(x) => Some(read[Map[String, Any]](x))
        case None => None
      }
    } catch {
      case _: Throwable => {
        logger.warn(s"error while parsing value of Secret $secretName to map")
        None
      }
    }
  }

  def getCredential(secretName: String, username: String): Option[String] = {
    val secretMap: Option[Map[String, Any]] = getSecretMapByName(secretName)
    try {
      secretMap match {
        case Some(x) => {
          x.get(username).asInstanceOf[Option[String]]
        }
        case None => None
      }
    } catch {
      case _: Throwable => None
    }
  }

}