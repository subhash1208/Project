package com.adp.datacloud.cli

import com.adp.datacloud.cli.DxrBaseConfig.getDxrConnectionPropertiesByWalletLocation
import com.adp.datacloud.ds.aws.SecretsStore
import oracle.jdbc.OracleConnection
import org.apache.log4j.Logger
import org.apache.spark.SparkFiles
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import java.util.Properties
import scala.io.Source

trait DxrBaseConfig {

  def dxrProductName: String

  def dxrJdbcUrl: String

  def applicationName: String

  def jdbcDBType: String

  def oracleWalletLocation: String

  def inputParams: Map[String, String]

  def dxrProductVersion: Option[String]

  def esUsername: Option[String]

  def esPwd: Option[String]

  def files: Option[String]

  //TODO: check if optional fields can be deserialized properly
  def toJson: String = DxrBaseConfig.toJson(this)

  def jdbcPrefix: String = {
    jdbcDBType match {
      case "oracle"    => "jdbc:oracle:thin"
      case "sqlserver" => "jdbc:sqlserver"
      case "mysql"     => "jdbc:mysql"
      case "postgres"  => "jdbc:postgresql"
      case "redshift"  => "jdbc:redshift"
      case _           => "jdbc:oracle:thin"
    }
  }

  def jdbcDriver: String = {
    jdbcDBType match {
      case "oracle"    => "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver"
      case "sqlserver" => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case "mysql"     => "com.mysql.cj.jdbc.Driver"
      case "postgres"  => "org.shadow.postgresql.Driver"
      case "redshift"  => "com.amazon.redshift.jdbc.Driver"
      case _           => "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver"
    }
  }

  lazy val dxrConnectionProperties: Properties = {
    getDxrConnectionPropertiesByWalletLocation(oracleWalletLocation)
  }

  def dxrSql: String = {
    val baseSql = if (dxrProductName.endsWith(".sql")) {

      val sourceFile = if (new java.io.File(dxrProductName).exists()) {
        // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
        Source.fromFile(dxrProductName)
      } else {
        Source.fromFile(SparkFiles.get(dxrProductName))
      }

      val sqlString =
        try sourceFile.mkString
        finally sourceFile.close()
      inputParams.get("env") match {
        case Some(x) => sqlString.replaceAll("&environment", x)
        case None    => sqlString
      }
    } else {
      val versionClause =
        if (dxrProductVersion.isDefined)
          s"AND c.product_version = '${dxrProductVersion.get}'"
        else s""

      s"""SELECT
            b.target_key,
            b.connection_id,
            b.connection_password,
            c.product_name,
            c.product_version,
            b.owner_id,
            a.dsn,
            b.client_identifier,
            b.session_setup_call,
            b.adpr_extension
        FROM instances a, targets b, products c
        WHERE
            a.instance_key = b.instance_key
            AND b.product_key = c.product_key
            AND b.active_flag in ('t','i')
            AND c.product_name = '${dxrProductName}'
            ${versionClause}
      """
    }
    "(\n" + inputParams.foldLeft(baseSql) { (y, x) =>
      y.replaceAll("&" + x._1, x._2)
    } + "\n)"
  }

  /**
   * returns a map of es credentials.
   * setting it as spark conf might show the password in spark UI better use it as runtime argument to es dataframe write method
   * @param sparkConf
   * @return
   */
  def getESCredentialsConfMap(sparkConfMap: Map[String, String]): Map[String, String] = {

    val esUserNameValue = if (esUsername.isDefined) {
      esUsername.get
    } else if (sparkConfMap.get("spark.es.net.http.auth.user").isDefined) {
      sparkConfMap("spark.es.net.http.auth.user")
    } else ""

    if (esUserNameValue.nonEmpty) {
      val esPassword = esPwd match {
        case Some(y) => y
        case None =>
          SecretsStore
            .getCredential("__ORCHESTRATION_ES_CREDENTIALS__", esUserNameValue)
            .get
      }
      Map(
        "spark.es.net.http.auth.user" -> esUserNameValue,
        "es.net.http.auth.user" -> esUserNameValue,
        "spark.es.net.http.auth.pass" -> esPassword,
        "es.net.http.auth.pass" -> esPassword)
    } else {
      Map[String, String]()
    }
  }

}

object DxrBaseConfig {

  // this implicit is not serializable, keeping it outside the trait to avoid issues in tasks
  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def toJson(config: DxrBaseConfig): String = {
    write(config)
  }

  def getDxrConnectionPropertiesByWalletLocation(
      oracleWalletLocation: String): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty(
      "driver",
      "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver")
    // pass the the jdbc url including user name and password if wallet is not setup for local mode
    // jdbc:oracle:thin:<UserName>/<password>@<hostname>:<port>/<serviceID>
    connectionProperties.setProperty(
      OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION,
      oracleWalletLocation)
    connectionProperties
  }
}
