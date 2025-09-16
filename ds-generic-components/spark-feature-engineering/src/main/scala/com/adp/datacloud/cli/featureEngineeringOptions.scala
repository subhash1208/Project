package com.adp.datacloud.cli

/**
 * Feature Engineering Command Line Options utility class.
 * Can be used by individual workflow classes as needed.
 */
case class Config(
    input: String                          = null,
    outputTable: String                    = null,
    outlierFilteredTable: Option[String]   = None,
    extendedFilters: Boolean               = true,
    groupingColumns: List[String]          = null,
    inputParams: List[String]              = List(),
    outputPartitionColumns: Option[String] = None,
    sparkShufflePartitions: Int            = 200,
    enableTungsten: Boolean                = true,
    checkpointDir: String                  = "/tmp/checkpoints/",
    enableCheckpoint: Boolean              = true,
    applicationName: String                = "ADP DataCloud - Feature Engineering",
    parquetMode: Boolean                   = false,
    saveFormat: String                     = "parquet") {

  def sql = {
    val sourcequery = if (input.endsWith(".sql")) {
      scala.io.Source
        .fromFile(input)
        .getLines()
        .map { _.replaceAll("\\-\\-.+", "").replaceAll("^\\s*$", "") }
        .filter(!_.isEmpty())
        .mkString("\n")
    } else {
      "select * from " + input
    }
    //Replacing the hiveconf variables
    inputParams.foldLeft(sourcequery) { (y, x) =>
      ("\\$\\{hiveconf:" + x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))
    }
  }

}

object featureEngineeringOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[Config]("Start") {
      head("Feature Engineering", this.getClass.getPackage().getImplementationVersion())

      override def showUsageOnError = true

      opt[String]('n', "application-name")
        .action((x, c) => c.copy(applicationName = x))
        .text("Name of the application.")

      opt[String]('i', "input-spec")
        .required()
        .action((x, c) => c.copy(input = x))
        .text("Input Either of 1) Fully qualified table or 2) Sql file or 3) Fully qualified HDFS path")

      opt[Boolean]('l', "local-parquet-mode")
        .action((x, c) => c.copy(parquetMode = x))
        .text(
          "Treat the inputs and outputs as local/hdfs paths to parquet files. Defaults to false. If true, the input-partitions-* parameters are ignored")

      opt[String]('f', "outlier-filtered-table")
        .action((x, c) => c.copy(outlierFilteredTable = Some(x)))
        .text(
          "Outliers filtered intermediate output Table. Fully qualified with domain name.")

      opt[String]('o', "output-table")
        .required()
        .action((x, c) => c.copy(outputTable = x))
        .text("Output Table. Fully qualified with domain name.")

      opt[Boolean]('e', "extended-outlier-filters")
        .action((x, c) => c.copy(extendedFilters = x))
        .text("Extended Outlier Filters. Default true")

      opt[String]('g', "grouping-columns")
        .required()
        .action((x, c) => c.copy(groupingColumns = x.split(",").toList))
        .text("Base columns to be used for group conditions")

      opt[String]('p', "hiveconf")
        .unbounded()
        .action((x, c) => c.copy(inputParams = c.inputParams ++ List(x)))
        .text("Hiveconf variables to replaced in input query")

      opt[String]('c', "output-partition-columns")
        .action((x, c) => c.copy(outputPartitionColumns = Some(x)))
        .text("Partition columns for the generated output table")

      opt[Int]('s', "num-shuffle-partitions")
        .action((x, c) => c.copy(sparkShufflePartitions = x))
        .text("Number of spark shuffle partitions to be used. Defaults to 200")

      opt[Boolean]('t', "enable-tungsten")
        .action((x, c) => c.copy(enableTungsten = x))
        .text(
          "Enable/Disable tungsten (true/false). Defaults to true. Disable this Spark 1.5 is being used")

      opt[String]('r', "reliable-checkpoint-dir")
        .action((x, c) => c.copy(checkpointDir = x))
        .text("RDD Checkpoint directory. Defaults to /tmp/checkpoints/")

      opt[String]('s', "save-format")
        .action((x, c) => c.copy(saveFormat = x))
        .text("Output Table format. Supported values are 'parquet' and 'text' ")
        .validate(x =>
          if (List("parquet", "text").contains(x)) success
          else failure("save-format option must be one of 'parquet' and 'text'"))

      help("help").text("prints this usage text")

      note("### ADP Feature Engineering Framework ###\n")

    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) => config
      case None => {
        System.exit(1)
        null // Error message would have been displayed
      }
    }

  }

}
