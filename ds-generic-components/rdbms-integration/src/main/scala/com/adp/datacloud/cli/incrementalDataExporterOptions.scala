package com.adp.datacloud.cli

import org.apache.spark.SparkFiles

import java.io.File
import scala.io.Source


case class IncrementalExporterConfig(
                                      xmlFilePath: String = null,
                                      dxrProductName:           String              = null,
                                      dxrProductVersion:        Option[String]      = None,
                                      dxrJdbcUrl:               String              = null,
                                      incrementalFlag:          Boolean             = true,
                                      jdbcDBType:               String              = "oracle",
                                      oracleWalletLocation:     String              = "/app/oraclewallet",
                                      greenDatabaseName:        String              = "__GREEN_RAW_DB__",
                                      blueDatabaseName:         String              = "__BLUE_RAW_DB__",
                                      landingDatabaseName:      String              = "__BLUE_LANDING_DB__",
                                      inputParams:              Map[String, String] = Map(),
                                      esUsername:               Option[String]      = None,
                                      esPwd:                    Option[String]      = None,
                                      files: Option[String]                = None) extends  DxrBaseConfig {

  def applicationName = {
    "INCR_EXPORT_"+xmlFilePath.toUpperCase().replace(".xml", "")
  }

  def xmlContent = {
    val absolutePath = if (new File(xmlFilePath).exists()) {
      xmlFilePath
    } else {
      SparkFiles.get(xmlFilePath)
    }
    val content = Source.fromFile(absolutePath)("UTF-8").getLines.mkString
    //Replacing the conf variables
    inputParams.foldLeft(content) { (y, x) =>
      ("\\{\\{" + x._1 + "\\}\\}").r.replaceAllIn(y, x._2)
    }
  }
}

object incrementalDataExporterOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[IncrementalExporterConfig]("ADP Incremental Data Exporter Utility") {

      head("ADP Delta Capture ", this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      opt[String]('c', "product-name").required().action((x, c) =>
        c.copy(dxrProductName = x)).text("dxrProduct Name or dxrsql file for which extract has to run")

      opt[String]('h', "dxr-jdbc-url").required().action((x, c) =>
        c.copy(dxrJdbcUrl = x)).text("Database Datasourcename (DNS)")

      opt[String]('w', "oracle-wallet-location").action((x, c) =>
        c.copy(oracleWalletLocation = x)).text("folder path of the oracle wallet. Defaults to /app/oraclewallet")

      opt[String]('s', "target-db").action((x, c) =>
        c.copy(jdbcDBType = x)).text("Source DB Type oracle/sqlserver. Defaults to oracle ")
      
      opt[String]('x', "xml-config-file").required().action((x, c) =>
        c.copy(xmlFilePath = x)).text("XML config file relative path from current working directory. Mandatory")
        
      opt[String]("green-db").required().action((x, c) =>
        c.copy(greenDatabaseName = x)).text("Target Green output Hive Database Name")

      opt[String]("blue-db").action((x, c) =>
        c.copy(blueDatabaseName = x)).text("Blue Database Name")

      opt[String]("landing-db").required().action((x, c) =>
        c.copy(landingDatabaseName = x)).text("Landing Database Name")

      opt[Boolean]('x', "incremental").action((x, c) =>
        c.copy(incrementalFlag = x)).text("Enable/Disable incremental processing")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")
      
      opt[String]("sql-conf").unbounded().action((x, c) =>
        c.copy(inputParams = c.inputParams + (x.split("=")(0) -> x.split("=").tail.mkString("=")))).text("Sqlconf variables to be replaced in XML config file")

      opt[String]("es-username").action((x, c) =>
        c.copy(esUsername = Some(x))).text("Elasticsearch username")

      opt[String]("es-password").action((x, c) =>
        c.copy(esPwd = Some(x))).text("Elasticsearch password. Not secure and hence not recommended. By default, password shall be lookedup from Secretstore")

      help("help").text("Prints usage")

    }

    parser.parse(args, IncrementalExporterConfig()).get

  }
}