package com.adp.datacloud.cli

import java.util.{Calendar, Properties}
import java.text.SimpleDateFormat
import com.adp.datacloud.ds.aws.ParameterStore
import oracle.jdbc.OracleConnection
import org.apache.spark.SparkFiles

case class ShapeMonitorConfig(
    xmlFilePath: String            = null,
    dbJdbcUrl: Option[String]      = null,
    dbUserName: Option[String]     = None,
    dbPassword: Option[String]     = None,
    oracleWalletLocation: String   = "/app/oraclewallet",
    rerun: Boolean                 = false,
    quickRun: Boolean              = true,
    targetTable: String            = "SHAPE_COLUMN_STATISTICS",
    targetValidationTable: String  = "SHAPE_VALIDATION_SUMMARY",
    environment: Option[String]    = None,
    partitionValue: Option[String] = None,
    parallelComputations: Int      = 2,
    inputParams: List[String]      = List(),
    files: Option[String]          = None) {

  def applicationName = {
    xmlFilePath.replace(".xml", "")
  }

  def xmlContent = {
    val sourceFile = if (new java.io.File(xmlFilePath).exists()) {
      scala.io.Source.fromFile(xmlFilePath)("UTF-8")
    } else {
      scala.io.Source.fromFile(SparkFiles.get(xmlFilePath))("UTF-8")
    }
    val content: String =
      try sourceFile.getLines.mkString
      finally sourceFile.close()
    //Replacing the conf variables
    inputParams.foldLeft(content) { (y, x) =>
      ("\\{\\{" + x.split("=")(0) + "\\}\\}").r.replaceAllIn(y, x.split("=")(1))
    }
  }

  lazy val jdbcUrl = {
    dbJdbcUrl match {
      case Some(url) => url
      case None => {
        // Get the DB from environment (AWS Systems Manager Parameter Store & AWS Secrets Manager)
        ParameterStore.getParameterByName("/test/__ENV_PREFIX__").getOrElse("MISSING")
      }
    }
  }

  lazy val jdbcProperties = {
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

  val yyyymm = partitionValue match {
    case Some(x) if { x.length() == 6 & "[0-9]{6}".r.findFirstMatchIn(x).isDefined } => x
    case Some(x) if {
          x.length() == 6 & "[0-9]{4}Q[1-4]".r.findFirstMatchIn(x).isDefined
        } =>
      x.slice(0, 4) + "%02d".format(x(5).asDigit * 3)
    case _ => {
      val calender = Calendar.getInstance()
      calender.add(Calendar.MONTH, -1)
      new SimpleDateFormat("YYYYMM").format(calender.getTime())
    }
  }

  val qtr = yyyymm.slice(0, 4) + 'Q' + (yyyymm.slice(4, 6).toInt / 3.0).ceil.toInt

}

object ShapeMonitorOptions {

  def parse(args: Array[String]) = {
    val parser =
      new scopt.OptionParser[ShapeMonitorConfig]("ADP DataShape Statistics Collector") {

        head(
          "ADP DataShape Statistics Collector and Shape Validator",
          this.getClass.getPackage.getImplementationVersion)

        override def showUsageOnError = true

        opt[String]('x', "xml-config-file")
          .required()
          .action((x, c) => c.copy(xmlFilePath = x))
          .text("XML config file relative path from current working directory. Mandatory")

        opt[String]('h', "db-jdbc-url")
          .action((x, c) => c.copy(dbJdbcUrl = Some(x)))
          .text("Database JDBC Url (with prefix). If not provided, defaults from AWS environment will be used")

        opt[String]('e', "environment")
          .action((x, c) => c.copy(dbJdbcUrl = Some(x)))
          .text("Logical environment. This is used to determine the default DB connection settings.")

        opt[String]('w', "oracle-wallet-location")
          .action((x, c) => c.copy(oracleWalletLocation = x))
          .text("Oracle Wallet location")

        opt[Boolean]('r', "rerun")
          .action((x, c) => c.copy(rerun = x))
          .text("Enforce Rerun of table stats")

        opt[Boolean]('q', "quick-run")
          .action((x, c) => c.copy(quickRun = x))
          .text(
            "Avails quick run of table stats, partition value is a must if true which is a default option")

        opt[String]('u', "db-username")
          .action((x, c) => c.copy(dbUserName = Some(x)))
          .text("Database UserName")

        opt[String]('p', "db-password")
          .action((x, c) => c.copy(dbPassword = Some(x)))
          .text("Database Password")

        opt[String]('t', "target-tablename")
          .action((x, c) => c.copy(targetTable = x))
          .text("Target table Name to write stats")

        opt[Int]('n', "parallel-computations")
          .action((x, c) => c.copy(parallelComputations = x))
          .text("Number of tables need to be computed in Parallel. Default is 2")

        opt[String]('v', "partition-value")
          .action((x, c) => c.copy(partitionValue = Some(x)))
          .text("Latest Temporal partition value in the data. It should be in the format of either YYYYMM or YYYYQQ. Eg. 201912, 2019Q4. Mandatory if quickrun is true")

        opt[String]('p', "conf")
          .unbounded()
          .action((x, c) => c.copy(inputParams = c.inputParams ++ List(x)))
          .text("Variables to replaced in sql")

        opt[String]("files")
          .action((x, c) => c.copy(files = Some(x)))
          .text("csv string dependent files with absolute paths")

        help("help").text("Prints usage")

        checkConfig(
          c =>
            if (c.jdbcUrl.isEmpty && c.environment.isEmpty) {
              failure(
                "Either of the three.. (environment) OR (jdbc-url, username, password) OR (jdbc-url, wallet) should be provided")
            } else
              success)

      }

    parser.parse(args, ShapeMonitorConfig()).get

  }
}
