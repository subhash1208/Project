package com.adp.datacloud.cli

import com.adp.datacloud.ds.aws.S3Utils.downloadS3FileToLocal
import org.apache.log4j.Logger
import org.apache.spark.SparkFiles

import scala.io.Source

/**
 * Cubing Framework Command Line Options utility class.
 */
case class CubeConfig(
    xmlFileName: String                 = null,
    input: String                       = null,
    outputDBName: String                = "default",
    outputPartitionSpec: Option[String] = None,
    overwriteDestination: Boolean       = true,
    scanDimensions: Boolean             = false,
    checkpointDir: String               = "/tmp/checkpoints/",
    distributionSpec: Option[String]    = None,
    localParquetInputMode: Boolean      = false,
    minCubeDepth: Int                   = 0,
    maxCubeDepth: Int                   = 99,
    ignoreNullGroupings: Boolean        = true,
    onlyPlan: Boolean                   = false,
    saveFormat: String                  = "parquet",
    inputParams: List[String]           = List(),
    multiColumnSpec: List[String]       = List(),
    limitedTestMode: Boolean            = false,
    files: Option[String]               = None) {

  private val logger = Logger.getLogger(getClass())

  def applicationName = {
    xmlFileName
      .replace(".xml", "")
      .replaceAll("\\.", "_") + "_" + minCubeDepth + "_" + maxCubeDepth
  }

  lazy val xmlConfigString = {
    val xmlSource = if (new java.io.File(xmlFileName).exists()) {
      // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
      Source.fromFile(xmlFileName)
    } else {
      try {
        Source.fromFile(SparkFiles.get(xmlFileName))
      } catch {
        // null pointer occurs if the file is accessed before spark context is created. temporary work around need to find a better fix
        case e: NullPointerException => {
          logger.warn(
            s"file $xmlFileName not found in spark files fetching it directly from s3",
            e)
          val xmlS3Path = files
            .getOrElse("")
            .trim
            .split(",")
            .filter(_.endsWith(xmlFileName))
            .head
          val xmlLocalPath = s"/tmp/s${System.currentTimeMillis()}/$xmlFileName"
          downloadS3FileToLocal(xmlS3Path, xmlLocalPath)
          Source.fromFile(xmlLocalPath)
        }
      }
    }
    val xmlString =
      try xmlSource.mkString
      finally xmlSource.close()
    logger.info(s"XML_CONFIG_STRING: $xmlString")
    xmlString
  }

  lazy val filesList: List[String] = {
    val filesString = files
      .getOrElse("")
      .trim

    if (filesString.nonEmpty) {
      filesString
        .split(",")
        .toList
    } else List[String]()
  }

  def cubeName = xmlFileName.replace(".xml", "").replaceAll("\\.", "_")

  lazy val sql = {
    {
      val sql = if (input.endsWith(".sql")) {
        val sourceFile = if (new java.io.File(input).exists()) {
          Source.fromFile(input)
        } else {
          Source.fromFile(SparkFiles.get(input))
        }
        "select * from (" + (try sourceFile.mkString
        finally sourceFile.close()) + "\n) ignored_alias"
      } else {
        "select * from " + input
      }

      val sqlString = if (!multiColumnSpec.isEmpty) {
        sql + " where " + multiColumnSpec.foldLeft("1=1") { (y, x) =>
          y + " and " + x.split("=")(0) + " in (" + x
            .split("=")(1)
            .split(",")
            .mkString("'", "', '", "'") + ")"
        }
      } else sql

      //Replacing the hiveconf variables
      inputParams.foldLeft(sqlString) { (y, x) =>
        ("\\$\\{hiveconf:" + x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))
      }

    }
  }

}

object CubeOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[CubeConfig]("CubeBuilder") {
      head("CubeBuilder", this.getClass.getPackage().getImplementationVersion())

      override def showUsageOnError = true

      opt[String]('x', "xml-config-file")
        .required()
        .action((x, c) => c.copy(xmlFileName = x))
        .text("XML config file name. Mandatory")

      opt[String]('i', "input-spec")
        .required()
        .action((x, c) => c.copy(input = x))
        .text("Input Either of 1) Fully qualified table or 2) Sql file or 3) Fully qualified HDFS path")

      opt[Boolean]('l', "local-parquet-input-mode")
        .action((x, c) => c.copy(localParquetInputMode = x))
        .text(
          "Treat the inputs and outputs as local/hdfs paths to parquet files. Defaults to false. If set to true, the input-partitions-* parameters are ignored")

      opt[String]('o', "output-db-name")
        .action((x, c) => c.copy(outputDBName = x))
        .text(
          "Hive Database name where output tables should be saved. Ignored if hdfs-output-path is provided")

      opt[String]('t', "output-partition-spec")
        .validate(x =>
          if (x.split("=").size <= 2) success
          else failure("Incorrect output-partition-spec"))
        .action((x, c) => c.copy(outputPartitionSpec = Some(x)))
        .text(
          "Output Partition Spec (column or column=value). Optional. Example pp=2017q1")

      opt[String]('m', "multi-column-spec")
        .unbounded()
        .action((x, c) => // --multi-column-spec yr=2015,2016,2017
          c.copy(multiColumnSpec = c.multiColumnSpec ++ List(x)))
        .text("Partition Spec")

      opt[String]('d', "distribute-by")
        .action((x, c) => c.copy(distributionSpec = Some(x)))
        .text("Distribute input by the specified columns prior to aggregate processing.")

      opt[Boolean]("limited-test-mode")
        .action((x, c) => c.copy(limitedTestMode = x))
        .text("Perform a limited test using only 10000 input rows")

      opt[Boolean]("overwrite-destination")
        .action((x, c) => c.copy(overwriteDestination = x))
        .text("Overwrite Destination Table and/or partition(s)")

      opt[Boolean]("only-plan")
        .action((x, c) => c.copy(onlyPlan = x))
        .text("Build/Explain Cube (true/false). Defaults to false.")

      opt[String]('r', "reliable-checkpoint-dir")
        .action((x, c) => c.copy(checkpointDir = x))
        .text("RDD Checkpoint directory. Defaults to /tmp/checkpoints/")

      opt[Int]("min-depth")
        .action((x, c) => c.copy(minCubeDepth = x))
        .text("Minimum Cube depth (non-null dimensions in each cell). Optional.")

      opt[Int]("max-depth")
        .action((x, c) => c.copy(maxCubeDepth = x))
        .text("Maximum Cube depth (non-null dimensions in each cell). Optional.")

      opt[Boolean]("ignore-null-groupings")
        .action((x, c) => c.copy(ignoreNullGroupings = x))
        .text(
          "Ignore groups having a null in atleast one of the dimensions (true by default). Optional.")

      opt[String]("hiveconf")
        .unbounded()
        .action((x, c) => c.copy(inputParams = c.inputParams ++ List(x)))
        .text("Hiveconf variables to replaced in input")

      opt[Boolean]('w', "warn-and-exit")
        .action((x, c) => c.copy(scanDimensions = x))
        .text(
          "Warn about possible issues prior to building and exit. This adds an pre-step to scan dimensions in the configuration. Defaults to true")

      opt[String]('s', "save-format")
        .action((x, c) => c.copy(saveFormat = x))
        .text("Output Table format. Supported values are 'parquet' and 'text' ")
        .validate(x =>
          if (List("parquet", "text").contains(x)) success
          else failure("save-format option must be one of 'parquet' and 'text'"))

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      help("help").text("Prints usage")

      note("ADP Cube Builder Framework. \n")

    }

    // parser.parse returns Option[C]
    parser.parse(args, CubeConfig()).get

  }

}
