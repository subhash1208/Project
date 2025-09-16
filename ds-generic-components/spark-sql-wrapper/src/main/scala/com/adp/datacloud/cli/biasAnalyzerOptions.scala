package com.adp.datacloud.cli

import org.apache.spark.SparkFiles

import scala.io.Source

case class BiasAnalyzerConfig(
    inputFilePath: String          = "",
    inputParams: List[String]      = List(),
    biasCheckColumns: List[String] = List(),
    groupByColumns: List[String]   = List(),
    esUsername: Option[String]     = None,
    esPwd: Option[String]          = None,
    frequency: String              = "",
    files: Option[String]          = None) {

  lazy val inputParamsMap: Map[String, String] = inputParams
    .map(param => {
      val tokens = param.split("=")
      (tokens(0) -> tokens(1))
    })
    .toMap

  def sql = {
    val sourcequery = (if (new java.io.File(inputFilePath).exists()) {
                         // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
                         Source.fromFile(inputFilePath)("UTF-8")
                       } else {
                         Source.fromFile(SparkFiles.get(inputFilePath))("UTF-8")
                       })
      .getLines()
      .map {
        _.replaceAll("^\\s*\\-\\-.*$", "").replaceAll("^\\s*$", "")
      }
      .filter(!_.isEmpty())
      .mkString("\n")

    //Replacing the hiveconf variables
    inputParamsMap
      .foldLeft(sourcequery) { (y, x) =>
        ("\\$\\{hiveconf:" + x._1 + "\\}").r
          .replaceAllIn(y, x._2)
      }
      .replaceAll("\\$\\{hiveconf:[a-zA-Z0-9]+\\}", "")
  }
}

object biasAnalyzerOptions {

  def parse(args: Array[String]) = {

    val parser = new scopt.OptionParser[BiasAnalyzerConfig]("Bias Analyzer") {

      head("Model Bias Analyzer", this.getClass.getPackage().getImplementationVersion())
      help("help").text("Prints usage")

      override def showUsageOnError = true

      opt[String]('f', "input-file-path")
        .required()
        .action((arg, config) => config.copy(inputFilePath = arg))
        .text("Input Sql string")

      opt[String]('c', "hiveconf")
        .unbounded()
        .action((arg, config) =>
          config.copy(inputParams = config.inputParams ++ List(arg)))
        .text("Hiveconf variables to replaced in query")

      opt[String]('b', "bias-check-column")
        .unbounded()
        .required()
        .action((arg, config) =>
          config.copy(biasCheckColumns = config.biasCheckColumns ++ List(arg)))
        .text("Column names for bias testing")

      opt[String]('b', "group-by-column")
        .unbounded()
        .action((arg, config) =>
          config.copy(groupByColumns = config.groupByColumns ++ List(arg)))
        .text("Industry Column name for testing bias across industries/sector")

      opt[String]('b', "frequency")
        .action((arg, config) => config.copy(frequency = arg))
        .text("Timestamp frequency required to write truncated timestamp to ES index")

      opt[String]("es-username")
        .action((x, c) => c.copy(esUsername = Some(x)))
        .text("Elasticsearch username")

      opt[String]("es-password")
        .action((x, c) => c.copy(esPwd = Some(x)))
        .text("Elasticsearch password. Not secure and hence not recommended. By default, password shall be lookedup from Secretstore")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      checkConfig(c => {
        val overlappingColumns =
          c.biasCheckColumns.toSet.intersect(c.groupByColumns.toSet)
        if (overlappingColumns.nonEmpty)
          failure(
            s"The values for 'bias-check-column' & 'industry-column' cannot have overlapping fields. ${overlappingColumns
              .mkString(",")}")
        else
          success
      })
    }

    parser.parse(args, BiasAnalyzerConfig()) match {
      case Some(config) => config
      case None => {
        System.exit(1)
        null // Error message would have been displayed
      }
    }

  }

}
