package com.adp.datacloud.cli

import org.apache.spark.SparkFiles

import scala.io.Source

case class ExporterTaskLog(APPLICATION_ID: String,
                           ATTEMPT_ID: Option[String],
                           TARGET_KEY: Integer,
                           DB_PARTITION: String,
                           DB_USER_NAME: String,
                           JDBC_URL: String,
                           OWNER_ID: String,
                           CLIENT_IDENTIFIER: String,
                           FINAL_SELECT_SQL: String,
                           FINAL_SESSION_SETUP_CALL: String,
                           MSG: String,
                           EXEC_TIME_MS: Integer = 0,
                           ROW_COUNT: Integer = 0,
                           IS_ERROR: Boolean = false)

case class DxrParallelDataExporterConfig(dxrProductName: String = null,
                                         dxrProductVersion: Option[String] = None,
                                         dxrJdbcUrl: String = null,
                                         jdbcDBType: String = "oracle",
                                         oracleWalletLocation: String = "/app/oraclewallet",
                                         hiveTable: String = null,
                                         hivePartitionColumn: Option[String] = None,
                                         hivePartitionValue: Option[String] = None,
                                         targetTable: String = null,
                                         useStagingTable: Boolean = false,
                                         inputParams: Map[String, String] = Map(),
                                         esUsername: Option[String] = None,
                                         esPwd: Option[String] = None,
                                         maxParallelDBConnections: Integer = 10,
                                         rdbmsBatchSize: Integer = 1000,
                                         stagingTableName: Option[String] = None,
                                         preExportSql: Option[String] = None,
                                         postExportSql: Option[String] = None,
                                         mergeColumns: Option[List[String]] = None,
                                         plsqlConnectionTimeOut: Int = 14400000, // 4hrs in milli seconds
                                         applicationName: String = "DXR Parallel Data Exporter",
                                         files: Option[String] = None,
                                         redshiftS3TempDir: Option[String] = None,
                                         redshiftIAMRole: Option[String] = None) extends DxrBaseConfig {

  def hiveSql = {
    s"select * from $hiveTable "
  }

  def getStringFromFile(sqlFileName: String) = {
    val sourceFile = if (new java.io.File(sqlFileName).exists()) {
      // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
      Source.fromFile(sqlFileName)
    } else {
      Source.fromFile(SparkFiles.get(sqlFileName))
    }
    val sqlString = try sourceFile.mkString
    finally sourceFile.close()
    sqlString
  }

  def getSqlFromFile(sqlFileName: String) = {
    val sqlString = getStringFromFile(sqlFileName)
    // Substitute sql-conf variables. Note that this is substitution and not parameter binding
    inputParams
      .foldLeft(sqlString) { (y, x) =>
        y.replaceAll("&" + x._1, x._2)
      }
      .replaceAll("&[a-zA-Z1-9_]+", "")
  }

}

object dxrParallelDataExporterOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[DxrParallelDataExporterConfig]("DXR Parallel Data Exporter") {
      head("DxrParallelRdbmsDataExporter", this.getClass.getPackage().getImplementationVersion())

      override def showUsageOnError = true

      opt[String]('c', "product-name").required().action((x, c) =>
        c.copy(dxrProductName = x)).text("dxrProduct Name or dxrsql file for which extract has to run")

      opt[String]('h', "dxr-jdbc-url").required().action((x, c) =>
        c.copy(dxrJdbcUrl = x)).text("Database Datasourcename (DNS)")

      opt[String]('w', "oracle-wallet-location").action((x, c) =>
        c.copy(oracleWalletLocation = x)).text("folder path of the oracle wallet. Defaults to /app/oraclewallet")

      opt[String]('s', "target-db").action((x, c) =>
        c.copy(jdbcDBType = x)).text("Source DB Type oracle/sqlserver. Defaults to oracle ")

      opt[String]('n', "application-name").action((x, c) =>
        c.copy(applicationName = x)).text("Name of the application.")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      opt[String]("pre-export-sql").action((x, c) =>
        c.copy(preExportSql = Some(x))).text("pre Export sql name")

      opt[String]("post-export-sql").action((x, c) =>
        c.copy(postExportSql = Some(x))).text("post Export sql name")

      opt[String]('h', "hive-table").required().action((x, c) =>
        c.copy(hiveTable = x)).text("Export Hive Table Name")

      opt[String]("hive-partition-column").action((x, c) =>
        c.copy(hivePartitionColumn = Some(x))).text("Export partition column name in the hive table. (Optional)")

      opt[String]("hive-partition-value").action((x, c) =>
        c.copy(hivePartitionValue = Some(x))).text("Export partition column value in the hive table. (Optional)")

      opt[String]('t', "target-rdbms-table").required().action((x, c) =>
        c.copy(targetTable = x)).text("Target warehouse table to which we need to export data")

      opt[Boolean]('s', "use-staging-table").action((x, c) =>
        c.copy(useStagingTable = x)).text("use staging table before copying data to target table")

      opt[String]('g', "staging-table-name").action((x, c) =>
        c.copy(stagingTableName = Some(x))).text("Name of the staging table.")

      opt[String]("merge-using-columns").action((x, c) =>
        c.copy(mergeColumns = Some(x.split(",").toList))).text("Use a merge operation with the specified columns(comma separated) instead of an insert. Only applicable when using a staging table")

      opt[String]("sql-conf").unbounded().action((x, c) =>
        c.copy(inputParams = c.inputParams + (x.split("=")(0) -> x.split("=").tail.mkString("=")))).text("Sqlconf variables to replaced in sql query")

      opt[String]("es-username").action((x, c) =>
        c.copy(esUsername = Some(x))).text("Elasticsearch username")

      opt[String]("es-password").action((x, c) =>
        c.copy(esPwd = Some(x))).text("Elasticsearch password. Not secure and hence not recommended. By default, password shall be lookedup from Secretstore")

      opt[Int]("plsql-timeout")
        .action((x, c) => c.copy(plsqlConnectionTimeOut = x * 1000))
        .validate(x =>
          if (x > 0) success
          else failure("Option --plsql-timeout must be >0")
        )
        .text("Time out for PLSQL executions in seconds. Defaults to 14400")

      //TODO: Create options for customizing the other parameters

      opt[String]("redshift-iam-role").action((x, c) =>
        c.copy(redshiftIAMRole = Some(x))).text("Redshift IAM role which can access s3 staging directory")

      opt[String]("redshift-s3-tempdir").action((x, c) =>
        c.copy(redshiftS3TempDir = Some(x))).text("Redshift S3 tempdir required for copy cmd")

      help("help").text("Prints usage")

      note("ADP Spark Based DXR Parallel RDBS Data Exporter. \n")
    }

    // parser.parse returns Option[C]
    parser.parse(args, DxrParallelDataExporterConfig()).get

  }
}
