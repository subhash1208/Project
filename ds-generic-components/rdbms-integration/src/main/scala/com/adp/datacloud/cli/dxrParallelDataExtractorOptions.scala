package com.adp.datacloud.cli

import org.apache.spark.SparkFiles

import scala.collection.immutable.ListMap
import scala.io.Source

case class DxrParallelDataExtractorConfig(
    dxrProductName: String               = null,
    dxrProductVersion: Option[String]    = None,
    dxrJdbcUrl: String                   = null,
    jdbcDBType: String                   = "oracle",
    optoutMaster: Option[String]         = None,
    inputParams: Map[String, String]     = Map(),
    esUsername: Option[String]           = None,
    esPwd: Option[String]                = None,
    outputPartitionSpec: Option[String]  = None,
    outputFormat: String                 = "parquet",
    applicationName: String              = "DXR Parallel Data Extractor",
    sqlFileNames: List[String]           = null,
    maxParallelConnectionsPerDb: Integer = 10,
    fetchArraySize: Int                  = 1000,
    batchColumn: Option[String]          = None,
    numBatches: Integer                  = 4,
    upperBound: Long                     = 200,
    lowerBound: Long                     = 0,
    limit: Int                           = 0, // Equivalent to no limit
    singleTargetOnly: Boolean            = false,
    skipStagingCleanup: Boolean          = false,
    queryTimeout: Int                    = 1800,
    retryIntervalSecs: Int               = 60,
    nullStrings: List[String]            = List("UNKNOWN", "NULL", "unknown", "null"),
    oracleWalletLocation: String         = "/app/oraclewallet",
    s3StagingBucket: Option[String]      = None,
    isolationLevel: Int                  = 2,
    optOutRerunSpec: Option[String]      = None,
    strictMode: Boolean                  = true,
    isOverWrite: Boolean                 = true,
    isVersioned:Boolean                  = false,
    doMaintenanceCheck: Boolean          = false,
    isManagedIncremental: Boolean        = false,
    compressionCodec: String             = "SNAPPY",
    greenDatabaseName: Option[String]    = None,
    blueDatabaseName: Option[String]     = None,
    landingDatabaseName: String          = "__BLUE_LANDING_DB__",
    skipGreenDb: Boolean                 = false,
    skipBlueDb: Boolean                  = false,
    extraHashColumnsList: List[String]   = List(),
    files: Option[String]                = None)
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

      // TODO: convert this dxr style place holders in sql
      val allInputParams = inputParams ++ Map(
        "UPPER_BOUND" -> upperBound.toString(),
        "upper_bound" -> upperBound.toString(),
        "LOWER_BOUND" -> lowerBound.toString(),
        "lower_bound" -> lowerBound.toString())

      val sqlString =
        try sourceFile.mkString
        finally sourceFile.close()
      val sortedAllInputParams = ListMap(allInputParams.toSeq.sortWith(_._1 > _._1): _*)
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

}

object dxrParallelDataExtractorOptions {

  def parse(args: Array[String]) = {
    val parser =
      new scopt.OptionParser[DxrParallelDataExtractorConfig](
        "DXR Parallel Data Extractor") {
        head(
          "DXR Parallel Data Extractor",
          this.getClass.getPackage.getImplementationVersion)

        override def showUsageOnError = true

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

        opt[String]("optout-master")
          .required()
          .action((x, c) =>
            c.copy(optoutMaster = if (x.toLowerCase != "none") Some(x) else None))
          .text("Optout Master table name. Set this to 'none' to disable optout. Use this responsibly and with caution")

        opt[String]("sql-files")
          .required()
          .action((x, c) => c.copy(sqlFileNames = x.split(",").toList))
          .text("List of SQL file names to be used for extract")

        // optional args

        opt[String]("sql-conf")
          .unbounded()
          .action((x, c) =>
            c.copy(inputParams =
              c.inputParams + (x.split("=")(0) -> x.split("=").tail.mkString("="))))
          .text("Hiveconf variables to replaced in query or in vpd")

        opt[String]("source-db")
          .action((x, c) => c.copy(jdbcDBType = x))
          .text("Source DB Type oracle/sqlserver/mysql/postgres")

        opt[String]("green-db")
          .action((x, c) =>
            c.copy(greenDatabaseName = if (x.toLowerCase != "none") Some(x) else None))
          .text("Target Green output Hive Database Name")

        opt[String]("blue-db")
          .action((x, c) =>
            c.copy(blueDatabaseName = if (x.toLowerCase != "none") Some(x) else None))
          .text("Blue Database Name")

        opt[Boolean]("skip-green-db")
          .action((x, c) => c.copy(skipGreenDb = x))
          .text(
            "Flag to skip green copy. Default value false")

        opt[Boolean]("skip-blue-db")
          .action((x, c) => c.copy(skipBlueDb = x))
          .text(
            "Flag to skip blue copy. Default value false")

        opt[String]("hash-columns")
          .action((x, c) =>
            c.copy(extraHashColumnsList = x.split(",").map(_.toLowerCase).toList))
          .text("List extra hash columns used when for writing dxr tables")

        opt[String]("files")
          .action((x, c) => c.copy(files = Some(x)))
          .text("csv string dependent files with absolute paths")

        opt[String]("output-partition-spec")
          .action((x, c) => c.copy(outputPartitionSpec = Some(x)))
          .text("Comma separated Output Partition Spec (column1,column2 or column1=value1,column2=value2).")

        opt[String]("output-format")
          .action((x, c) => c.copy(outputFormat = x))
          .text(
            "output format for the data to be stored. default to 'parquet' other options are 'delta'")

        opt[String]("batch-column")
          .action((x, c) => c.copy(batchColumn = Some(x)))
          .text("The column name to be used for batching retrievals.")

        opt[Int]("upper-bound")
          .action((x, c) => c.copy(upperBound = x))
          .text("Optional Upper bound when using batching. Defaults to 200")

        opt[Int]("lower-bound")
          .action((x, c) => c.copy(lowerBound = x))
          .text("Optional Lower bound when using batching. Defaults to 0")

        opt[Int]("fetch-array-size")
          .action((x, c) => c.copy(fetchArraySize = x))
          .text(
            "Fetch array size for extracts. Set this carefully as it can impact performance. Defaults to 1000")

        opt[Int]("num-batches")
          .action((x, c) => c.copy(numBatches = x))
          .text(
            "Number of partitions(or batches) for retrieval for each 'DXR target'. Ignored when batch-column is used. Defaults to 4. At maximum these many queries shall be fired on any given 'DXR target' per SQL")

        opt[String]("application-name")
          .action((x, c) => c.copy(applicationName = x))
          .text("Name of the application.")

        opt[Int]("max-db-parallel-connections")
          .action((x, c) => c.copy(maxParallelConnectionsPerDb = x))
          .text("Max Number of Parallel connections per DB. Defaults to 10")

        opt[String]("null-strings")
          .action((x, c) => c.copy(nullStrings = x.split(",").toList))
          .text("Comma separated list of string literals which shall be considered as equivalent to NULLs. Defaults to 'UNKNOWN,NULL,unknown,null'")

        opt[Int]("persql-limit")
          .action((x, c) => c.copy(limit = x))
          .text(
            "Development only setting to limit the number of records extracted from each SQL. Default 0 (no limit).")

        opt[Boolean]("single-target-only")
          .action((x, c) => c.copy(singleTargetOnly = x))
          .text(
            "Perform the extract from only a single target among possible targets. Useful while retrieving metadata from one of the possible connections")

        opt[Boolean]("skip-staging-cleanup")
          .action((x, c) => c.copy(skipStagingCleanup = x))
          .text(
            "skip cleaning up the staging files. Useful for performance testing writes. Default false")

        opt[Int]("query-timeout")
          .action((x, c) => c.copy(queryTimeout = x))
          .text("JDBC Query Timeout in seconds. Defaults to 1800 seconds")

        opt[Int]("retry-interval-secs")
          .action((x, c) => c.copy(retryIntervalSecs = x))
          .text("Interval before retrying the connection. Defaults to 60 seconds")

        opt[Int]("isolation-level")
          .action((x, c) => c.copy(isolationLevel = x))
          .text(
            "Transaction isolation level while reading data from target data bases. possible values are 0,1,2,4 and 8. Default value is 2")

        opt[String]("compression-codec")
          .action((x, c) => c.copy(compressionCodec = x))
          .text(
            "Compression Codec. Default is SNAPPY. Set this field to 'UNCOMPRESSED' to disable compression")

        opt[String]("oracle-wallet-location")
          .action((x, c) => c.copy(oracleWalletLocation = x))
          .text("Oracle wallet location. Defaults to /app/oraclewallet.")

        opt[String]("s3-staging-bucket")
          .action((x, c) => c.copy(s3StagingBucket = Some(x)))
          .text("S3 bucket to be used for holding staging data")

        opt[String]("optout-rerun-spec")
          .action((x, c) => c.copy(optOutRerunSpec = Some(x)))
          .text("Specify the WHERE condition need to applied while reading Blue Landing DB table to rerun opted-out data to GREEN/BLUE zones. By defalut full data will be considered. Eg. --optout-rerun-spec ingest_month>201909")

        opt[Boolean]("strict-mode")
          .action((x, c) => c.copy(strictMode = x))
          .text(
            "Mark the dxr job as failure in case of connection failures. Default value true")

        opt[Boolean]("overwrite")
          .action((x, c) => c.copy(isOverWrite = x))
          .text(
            "Flag to control if the target data is overwritten or appended to the target table. Default value true")

        opt[Boolean]("is-versioned")
          .action((x, c) => c.copy(isVersioned = x))
          .text(
            "Flag to control if the table is using legacy dxr versioning or delta conversion")

        opt[Boolean]("maintenance-check")
          .action((x, c) => c.copy(doMaintenanceCheck = x))
          .text(
            "Flag to control if a maintenance check has to be performed before we triggering the extract. Default value is false")

        opt[Boolean]("managed-incremental")
          .action((x, c) => c.copy(isManagedIncremental = x))
          .text("Flag to let dxr manage the incremental dates. Default false")

        opt[String]("es-username")
          .action((x, c) => c.copy(esUsername = Some(x)))
          .text("Elasticsearch username")

        opt[String]("es-password")
          .action((x, c) => c.copy(esPwd = Some(x)))
          .text("Elasticsearch password. Not secure and hence not recommended. By default, password shall be lookedup from Secretstore")

        checkConfig(
          c =>
            if ((c.greenDatabaseName.isDefined && c.blueDatabaseName.isDefined) && (c.blueDatabaseName.get == c.greenDatabaseName.get))
              failure("Blue Database cannot be the same as Green Database")
            else if (c.blueDatabaseName.isDefined && c.blueDatabaseName.get == c.landingDatabaseName)
              failure("Blue Database cannot be the same as Landing Database")
            else if (c.greenDatabaseName.isDefined && c.landingDatabaseName == c.greenDatabaseName.get)
              failure("Landing Database cannot be the same as Green Database")
            else success)

      }

    parser.parse(args, DxrParallelDataExtractorConfig()).get
  }

}
