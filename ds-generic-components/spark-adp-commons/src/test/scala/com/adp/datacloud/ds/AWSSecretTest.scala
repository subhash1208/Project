package com.adp.datacloud.ds

import com.adp.datacloud.util.SparkTestWrapper
import com.amazonaws.services.secretsmanager.model._
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.util.Base64

@RunWith(classOf[JUnitRunner])
class AWSSecretTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  def getSecretString(secretName: String, region: String = "us-east-1") = {
    val getSecretValueResult =
      secretsClient.getSecretValue(new GetSecretValueRequest().withSecretId(secretName))
    getSecretValueResult.getSecretString
  }

  test("fetch secret from secret store") {
    val secretName = "__DXR_RO_JDBC_URL__"
    val region     = "us-east-1"

    // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    // We rethrow the exception by default.

    val secret =
      try {
        val getSecretValueResult =
          secretsClient.getSecretValue(
            new GetSecretValueRequest().withSecretId(secretName))
        // Decrypts secret using the associated KMS CMK.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString != null)
          getSecretValueResult.getSecretString
        else
          new String(Base64.getDecoder.decode(getSecretValueResult.getSecretBinary).array)
      } catch {
        case e @ (_: DecryptionFailureException | _: InternalServiceErrorException |
            _: InvalidParameterException | _: InvalidRequestException |
            _: ResourceNotFoundException) =>
          throw e
        case e: Exception =>
          // We can't find the resource that you asked for.
          throw e
      }

    assert(secret != null || secret.nonEmpty)

  }

}
