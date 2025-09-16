package com.adp.datacloud.util

import com.amazonaws.SDKGlobalConfiguration.{
  ACCESS_KEY_SYSTEM_PROPERTY,
  SECRET_KEY_SYSTEM_PROPERTY
}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.{
  AssumeRoleRequest,
  AssumeRoleResult,
  GetCallerIdentityRequest,
  GetCallerIdentityResult
}
import org.apache.log4j.Logger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AssumeRoleTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val osName = System.getProperty("os.name").toLowerCase()
    if (osName
        .contains("windows") || osName
        .startsWith("mac")) {
      System.setProperty("http.proxyHost", "localhost")
      System.setProperty("http.proxyPort", "3128")
      System.setProperty(
        "http.nonProxyHosts",
        "localhost|127.0.0.*|10.*|192.168.*|ssov3.adpcorp.com|bitbucket.es.ad.adp.com|cdladpinexus01.es.ad.adp.com|cdladpinexus01|artifactory.us.caas.oneadp.com|cdldfoaff-scan.es.ad.adp.com|internal-es-alb*")
      System.setProperty("https.proxyHost", "localhost")
      System.setProperty("https.proxyPort", "3128")
      System.setProperty(
        "https.nonProxyHosts",
        "localhost|127.0.0.*|10.*|192.168.*|ssov3.adpcorp.com|bitbucket.es.ad.adp.com|cdladpinexus01.es.ad.adp.com|cdladpinexus01|artifactory.us.caas.oneadp.com|cdldfoaff-scan.es.ad.adp.com|internal-es-alb*")
    }
  }

  private val logger = Logger.getLogger(getClass())

  test("assume role") {
    //    val AWS_ROLE_SESSION_NAME_env = System.getenv("AWS_ROLE_SESSION_NAME")
    //    val AWS_ROLE_ARN_env = System.getenv("AWS_ROLE_ARN")
    val assumeRoleARN         = "arn:aws:iam::942420684378:role/LOG_READ_ONLY"
    val assumeRoleSessionName = "unittest-assume-role"

    // Creating the STS client is part of your trusted code. It has
    // the security credentials you use to obtain temporary security credentials.

    //    val r: GetCallerIdentityResult = AWSSecurityTokenServiceClientBuilder
    //      .defaultClient()
    //      .getCallerIdentity(new GetCallerIdentityRequest)
    //    logger.info(s"account: ${r.getAccount}")
    //    logger.info(s"userId:  ${r.getUserId}")
    //    logger.info(s"arn:     ${r.getArn}")

    val roleRequest: AssumeRoleRequest =
      new AssumeRoleRequest()
        .withRoleArn(assumeRoleARN)
        .withRoleSessionName(assumeRoleSessionName)
    val roleResponse: AssumeRoleResult =
      AWSSecurityTokenServiceClientBuilder.defaultClient().assumeRole(roleRequest)

    // Obtain credentials for the IAM role. Note that you cannot assume the role of an AWS root account;
    val sessionCredentials = roleResponse.getCredentials

    /**
     *
     * This makes the default credential provider chain to pick the cross account
     * [[https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html]]
     * [[https://docs.aws.amazon.com/AmazonS3/latest/userguide/AuthUsingTempSessionToken.html]]
     */

    System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, sessionCredentials.getAccessKeyId)
    System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, sessionCredentials.getSecretAccessKey)
    System.setProperty("aws.sessionToken", sessionCredentials.getSecretAccessKey)

    val rAssume: GetCallerIdentityResult = AWSSecurityTokenServiceClientBuilder
      .defaultClient()
      .getCallerIdentity(new GetCallerIdentityRequest)
    logger.info(s"assume role account: ${rAssume.getAccount}")
    logger.info(s"assume role userId:  ${rAssume.getUserId}")
    logger.info(s"assume role arn:     ${rAssume.getArn}")

    // Create a BasicSessionCredentials object that contains the credentials you just retrieved.
    val roleCredentials = new BasicSessionCredentials(
      sessionCredentials.getAccessKeyId,
      sessionCredentials.getSecretAccessKey,
      sessionCredentials.getSessionToken)

    // Provide temporary security credentials so that the client
    // can send authenticated requests to Amazon. You create the client
    // using the sessionCredentials object.
    val credentialProvider = new AWSStaticCredentialsProvider(roleCredentials)

    val rAssumeRole: GetCallerIdentityResult = AWSSecurityTokenServiceClientBuilder
      .standard()
      .withCredentials(credentialProvider)
      .build()
      .getCallerIdentity(new GetCallerIdentityRequest)
    logger.info(s"assume role credentials account: ${rAssumeRole.getAccount}")
    logger.info(s"assume role credentials userId:  ${rAssumeRole.getUserId}")
    logger.info(s"assume role credentials arn:     ${rAssumeRole.getArn}")
  }

}
