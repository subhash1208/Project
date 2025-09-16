package com.adp.datacloud.ds.rdbms

import oracle.jdbc.OracleConnection

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.JavaConverters.propertiesAsScalaMapConverter

object dxrOracleWalletTest {

  /*
   * copy oracle wallet from /app/oraclewallet in the gate way machine.
   * place it in your home directory or place it anywhere and change the path reference appropriately
   * */

  def main(args: Array[String]) {
    val userHomeDir = System.getProperty("user.home")
    val driver      = "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver"
    val url         = "jdbc:oracle:thin:@cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1"
    val dxrSql =
      s"""
        |SELECT
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
        FROM
            instances a,
            targets b,
            products c
        WHERE
            a.instance_key = b.instance_key
            AND   b.product_key = c.product_key
            AND   product_name = 'DXR_TESTING'
        |""".stripMargin

    System.getProperties.asScala.map(println)

    var connection: Connection = null

    try {
      Class.forName(driver)
      val connProperties = new Properties()
      connProperties.setProperty(
        "driver",
        "com.adp.datacloud.ds.rdbms.VPDEnabledOracleDriver")
      connProperties.setProperty(
        OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION,
        s"$userHomeDir/oraclewallet")
      connection = DriverManager.getConnection(url, connProperties)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(dxrSql)
      val metaData  = resultSet.getMetaData
      while (resultSet.next()) {
        val targetKey = resultSet.getString("target_key")
        println(targetKey)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      connection.close()
    }
  }
}
