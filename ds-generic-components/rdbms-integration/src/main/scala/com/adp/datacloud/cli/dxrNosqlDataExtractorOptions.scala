package com.adp.datacloud.cli

import org.apache.spark.SparkFiles
import scopt.OptionParser

import scala.collection.immutable.ListMap
import scala.io.Source

case class DxrNosqlDataExtractorConfig(
    dxrProductName: String              = null,
    dxrJdbcUrl: String                  = null,
    applicationName: String             = null,
    sqlFileNames: List[String]          = null,
    jdbcDBType: String                  = "mongo", // support dynamo in future
    oracleWalletLocation: String        = "/app/oraclewallet",
    optoutMaster: Option[String]        = None,
    outputPartitionSpec: Option[String] = None,
    outputFormat: String                = "parquet",
    strictMode: Boolean                 = true,
    inputParams: Map[String, String]    = Map(),
    readOptions: Map[String, String]    = Map(),
    dxrProductVersion: Option[String]   = None,
    esUsername: Option[String]          = None,
    esPwd: Option[String]               = None,
    greenDatabaseName: Option[String]   = None,
    blueDatabaseName: Option[String]    = None,
    landingDatabaseName: String         = "__BLUE_LANDING_DB__",
    isOverWrite: Boolean                = true,
    isManagedIncremental: Boolean       = false,
    extraHashColumnsList: List[String]  = List(),
    files: Option[String]               = None)
    extends DxrBaseConfig {

  lazy val sqlStringsMap: Map[String, String] = {
    sqlFileNames.map(name => {

      // TODO: read into dataframe using sparkSession.sparkContext.wholeTextFiles(SparkFiles.getRootDirectory()+"/*.sql",5).toDF("fileName","text")
      val sourceFile = if (new java.io.File(s"$name.sql").exists()) {
        // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
        Source.fromFile(s"$name.sql")
      } else {
        Source.fromFile(SparkFiles.get(s"$name.sql"))
      }

      val sqlString =
        try sourceFile.mkString
        finally sourceFile.close()
      val sortedAllInputParams = ListMap(inputParams.toSeq.sortWith(_._1 > _._1): _*)
      println(sortedAllInputParams)
      // Substitute sqlconf variables. Note that this is substitution and not parameter binding
      val finalSqlString = sortedAllInputParams
        .foldLeft(sqlString) { (y, x) =>
          y.replaceAll("&" + x._1, x._2)
        }
        .replaceAll("&[a-zA-Z1-9_]+", "")

      name -> finalSqlString
    })
  }.toMap

  def getStringFromFile(sqlFileName: String) = {
    val sourceFile = if (new java.io.File(sqlFileName).exists()) {
      // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
      Source.fromFile(sqlFileName)
    } else {
      Source.fromFile(SparkFiles.get(sqlFileName))
    }
    val sqlString =
      try sourceFile.mkString
      finally sourceFile.close()
    sqlString
  }

  def getSqlFromFile(sqlFileName: String) = {
    val sqlString = getStringFromFile(sqlFileName + ".sql")
    // Substitute sql-conf variables. Note that this is substitution and not parameter binding
    inputParams
      .foldLeft(sqlString) { (y, x) =>
        y.replaceAll("&" + x._1, x._2)
      }
      .replaceAll("&[a-zA-Z1-9_]+", "")
  }
}

object dxrNosqlDataExtractorOptions {

  def parse(args: Array[String]): DxrNosqlDataExtractorConfig = {
    val parser = {
      new OptionParser[DxrNosqlDataExtractorConfig]("DXR Document DB data Extractor") {

        head("dxrDocumentDBExtractor", this.getClass.getPackage.getImplementationVersion)
        override def showUsageOnError = true

        // required args

        opt[String]("product-name")
          .required()
          .action((x, c) => c.copy(dxrProductName = x))
          .text("dxrProduct Name or dxrsql file for which extract has to run")

        opt[String]("dxr-jdbc-url")
          .required()
          .action((x, c) => c.copy(dxrJdbcUrl = x))
          .text("DXR Database JDBC URL")

        opt[String]("landing-db")
          .required()
          .action((x, c) => c.copy(landingDatabaseName = x))
          .text("Landing Database Name")

        opt[String]("sql-files")
          .required()
          .action((x, c) => c.copy(sqlFileNames = x.split(",").toList))
          .text("List of SQL file names to be used for extract")

        // optional args

        opt[String]("hash-columns")
          .action((x, c) =>
            c.copy(extraHashColumnsList = x.split(",").map(_.toLowerCase).toList))
          .text("List extra hash columns used when for writing dxr tables")

        opt[String]("files")
          .action((x, c) => c.copy(files = Some(x)))
          .text("csv string dependent files with absolute paths")

        opt[String]("green-db")
          .action((x, c) =>
            c.copy(greenDatabaseName = if (x.toLowerCase != "none") Some(x) else None))
          .text("Target Green output Hive Database Name")

        opt[String]("blue-db")
          .action((x, c) =>
            c.copy(blueDatabaseName = if (x.toLowerCase != "none") Some(x) else None))
          .text("Blue Database Name")

        opt[String]("source-db")
          .action((x, c) => c.copy(jdbcDBType = x))
          .text("Source DB Type Default Mongo. TODO support dynamo DB")

        opt[Boolean]("strict-mode")
          .action((x, c) => c.copy(strictMode = x))
          .text(
            "Mark the dxr job as failure in case of connection failures. Default value true")

        opt[String]("sql-conf")
          .unbounded()
          .action((x, c) =>
            c.copy(inputParams =
              c.inputParams + (x.split("=")(0) -> x.split("=").tail.mkString("="))))
          .text("Hiveconf variables to replaced in query or in vpd")

        opt[String]('r', "read-option")
          .unbounded()
          .action((arg, cnf) => {
            cnf.copy(readOptions =
              cnf.readOptions + (arg.split("=")(0) -> arg.split("=").tail.mkString("=")))
          })
          .text("read options to the driver for extract")

        opt[String]("optout-master")
          .action((arg, cnf) => {
            cnf.copy(optoutMaster = if (arg.toLowerCase != "none") Some(arg) else None)
          })
          .text("Optout Master table name. Set this to 'none' to disable optout. Use this responsibly and with caution")

        opt[String]("output-partition-spec")
          .action((x, c) => c.copy(outputPartitionSpec = Some(x)))
          .text("Comma separated Output Partition Spec (column1,column2 or column1=value1,column2=value2).")

        opt[String]("output-format")
          .action((x, c) => c.copy(outputFormat = x))
          .text(
            "output format for the data to be stored. default to 'parquet' other options are 'delta'")

        opt[String]("application-name")
          .action((x, c) => c.copy(applicationName = x))
          .text("Name of the application.")

        opt[String]("oracle-wallet-location")
          .action((x, c) => c.copy(oracleWalletLocation = x))
          .text("Oracle wallet location. Defaults to /app/oraclewallet.")

        opt[Boolean]("overwrite")
          .action((x, c) => c.copy(isOverWrite = x))
          .text(
            "Flag to control if the target data is overwritten or appended to the target table. Default value true")

        opt[Boolean]("managed-incremental")
          .action((x, c) => c.copy(isManagedIncremental = x))
          .text("Flag to let dxr manage the incremental dates. Default false")

        opt[String]("es-username")
          .action((x, c) => c.copy(esUsername = Some(x)))
          .text("Elasticsearch username")

        opt[String]("es-password")
          .action((x, c) => c.copy(esPwd = Some(x)))
          .text("Elasticsearch password. Not secure and hence not recommended. By default, password shall be lookedup from Secretstore")
      }
    }
    parser.parse(args, DxrNosqlDataExtractorConfig()).get
  }

}
