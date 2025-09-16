package com.adp.datacloud.util

import com.amazonaws.services.s3.model.{
  CannedAccessControlList,
  GetObjectRequest,
  ObjectMetadata
}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.secretsmanager.{
  AWSSecretsManager,
  AWSSecretsManagerClientBuilder
}
import com.amazonaws.services.securitytoken.model.{
  GetCallerIdentityRequest,
  GetCallerIdentityResult
}
import com.amazonaws.services.securitytoken.{
  AWSSecurityTokenService,
  AWSSecurityTokenServiceClientBuilder
}
import com.amazonaws.services.simplesystemsmanagement.model.{
  GetParameterRequest,
  ParameterNotFoundException
}
import com.amazonaws.services.simplesystemsmanagement.{
  AWSSimpleSystemsManagement,
  AWSSimpleSystemsManagementClientBuilder
}
import org.apache.log4j.Logger

import java.io.File
import scala.sys.process._

/**
 * Base trait that contains resources for testing
 */
trait BaseTestWrapper {

  private val logger = Logger.getLogger(getClass())

  // check aws token before proceeding with tests
  checkAWSToken()

  val baseResourcesFile = new File("src/test/resources")
  val projectRootPath   = new File(".").getCanonicalPath

  val baseResourceAbsolutePath = baseResourcesFile.getAbsolutePath

  lazy val oracleWalletPath: String = {

    val oracleWalletDir = new File(
      baseResourceAbsolutePath + File.separator + s"oraclewallet")
    if (!oracleWalletDir.exists() || !oracleWalletDir.isDirectory) {
      val codeStoreBucket  = codeStoreBucketAWSParam
      val oracleWalletS3ey = oracleWalletAWSParam

      val oracleWalletFile = new File(s"$baseResourceAbsolutePath/wallet.tar.gz")

      val s3Object: ObjectMetadata = s3Client.getObject(
        new GetObjectRequest(codeStoreBucket, oracleWalletS3ey),
        oracleWalletFile)

      // TODO: find a pure scala solution, the below command doesn't work in windows
      val cmd =
        s"tar -C ${baseResourcesFile.getAbsolutePath} -xzf ${oracleWalletFile.getPath}"
      cmd.!
    }
    oracleWalletDir.getAbsolutePath
  }

  /**
   * set the proxy only if its local testing.
   * proxybypass list in java seems to be unreliable better update cntlm proxy with bypass proxy list
   * assuming the developer has cnltm proxy setup in this local system
   */
  def setLocalProxy(): Unit = {
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

  def clearLocalProxy(): Unit = {
    System.clearProperty("http.proxyHost")
    System.clearProperty("http.proxyHost")
    System.clearProperty("http.proxyPort")
    System.clearProperty("http.nonProxyHosts")
    System.clearProperty("https.proxyHost")
    System.clearProperty("https.proxyPort")
    System.clearProperty("https.nonProxyHosts")
  }

  def checkAWSToken(): Unit = {
    // set proxy before any api calls
    setLocalProxy()
    logger.info(s"user.home property value is ${System.getProperty("user.home")}")
    logger.info(
      s"AWS_CREDENTIAL_PROFILES_FILE env value is ${System.getenv("AWS_CREDENTIAL_PROFILES_FILE")}")
    val r: GetCallerIdentityResult = AWSSecurityTokenServiceClientBuilder
      .defaultClient()
      .getCallerIdentity(new GetCallerIdentityRequest)
    logger.info(s"account: ${r.getAccount}")
    logger.info(s"userId:  ${r.getUserId}")
    logger.info(s"arn:     ${r.getArn}")
    require(
      r.getAccount == "708035784431",
      "aws role for 708035784431 must to set for unit tests to work")
  }

  def saveAsFileTos3(
      s3Client: AmazonS3,
      stringObjKeyName: String,
      bucketName: String,
      fileContent: String): Unit = {
    try {
      s3Client.putObject(bucketName, stringObjKeyName, fileContent)
      s3Client.setObjectAcl(
        bucketName,
        stringObjKeyName,
        CannedAccessControlList.BucketOwnerFullControl)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def getPathByResourceName(
      resourceName: String,
      pathPrefix: String = File.separator): String = {
    getClass.getResource(pathPrefix + resourceName).getPath
  }

  // always keep aws clients lazy so that the proxy is set before init

  lazy val s3Client: AmazonS3 =
    AmazonS3ClientBuilder.defaultClient()

  lazy val ssmClient: AWSSimpleSystemsManagement =
    AWSSimpleSystemsManagementClientBuilder.defaultClient()

  lazy val stsClient: AWSSecurityTokenService =
    AWSSecurityTokenServiceClientBuilder.defaultClient()

  lazy val secretsClient: AWSSecretsManager =
    AWSSecretsManagerClientBuilder.defaultClient()

  /**
   * initialize SSM parameters
   */

  lazy val codeStoreBucketAWSParam =
    getParameterByName(
      "__CODESTORE_BUCKET__"
    ) // no default needed, this param exists in all accounts

  // aws parameters use DIT values as defaults until accessing DIT params from jenkins is resolved
  lazy val dxrDNSAWSParam =
    getParameterByName("__DXR_DSN__", "cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1")
  lazy val oracleWalletAWSParam =
    getParameterByName("/DIT/__ORACLE_WALLET__", "artifacts/wallet.tar.gz")

  lazy val esPortAWSParam = getParameterByName("__ORCHESTRATION_ES_PORT__", "9200")
  lazy val esUserName     = getParameterByName("__ORCHESTRATION_ES_USERNAME__", "ds_ingest")
  lazy val esProtocolAWSParam =
    getParameterByName("__ORCHESTRATION_ES_PROTOCOL__", "http")
  lazy val esDomainAWSParam = getParameterByName(
    "__ORCHESTRATION_ES_DOMAIN__",
    "internal-es-alb-467074288.us-east-1.elb.amazonaws.com")

  lazy val mongoPortAWSParam = getParameterByName("__DATASCIENCE_DOCDB_PORT__", "20003")
  lazy val mongoDomainAWSParam = getParameterByName(
    "__DATASCIENCE_DOCDB_HOST__",
    "dsdocdb-services-nonprod.cluster-chrl8msuhuwz.us-east-1.docdb.amazonaws.com")

  def getParameterByName(
      paramName: String,
      defaultValue: String = "NA",
      decryption: Boolean  = false): String = {

    val getParamsReq =
      new GetParameterRequest().withName(paramName).withWithDecryption(decryption)

    try {
      val getParameterResult = ssmClient.getParameter(getParamsReq)
      getParameterResult.getParameter.getValue
    } catch {
      case en: ParameterNotFoundException => {
        logger.warn(
          s"Parameter: $paramName not found in parameter store returning default value: $defaultValue")
        defaultValue
      }
    }

  }

}
