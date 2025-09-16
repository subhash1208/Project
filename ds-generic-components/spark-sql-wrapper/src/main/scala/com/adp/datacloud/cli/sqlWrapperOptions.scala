package com.adp.datacloud.cli

import org.apache.spark.SparkFiles
import scala.io.Source

case class SqlWrapperConfig(
  file:                   Option[String] = None,
  sqlString:              String         = "",
  inputParams:            List[String]   = List(),
  optimizeInsert:         Boolean        = false,
  files: Option[String]                = None) {

  def sql = {
    val sourcequery = file match {
      case Some(x) => {
        (if (new java.io.File(x).exists()) {
          // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
          Source.fromFile(x)("UTF-8")
        } else {
          Source.fromFile(SparkFiles.get(x))("UTF-8")
        }).getLines().map { _.replaceAll("^\\s*\\-\\-.*$", "").replaceAll("^\\s*$", "") }.filter(!_.isEmpty()).mkString("\n")
      }
      case None => sqlString
    }
    //Replacing the hiveconf variables
    inputParams.foldLeft(sourcequery) { (y, x) =>
      ("\\$\\{hiveconf:" + x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))
    }.replaceAll("\\$\\{hiveconf:[a-zA-Z0-9]+\\}", "")
  }
}
object sqlWrapperOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[SqlWrapperConfig]("Sql/Hive Query Executor") {
      head("Sql/Hive Query Executor", this.getClass.getPackage().getImplementationVersion())

      override def showUsageOnError = true

      opt[String]('f', "file").action((x, c) =>
        c.copy(file = Some(x))).text("Input Sql file")

      opt[String]('e', "execute").action((x, c) =>
        c.copy(sqlString = x)).text("Sql string to execute")

      opt[String]('p', "hiveconf").unbounded().action((x, c) =>
        c.copy(inputParams = c.inputParams ++ List(x))).text("Hiveconf variables to replaced in query")

      opt[Boolean]('o', "optimize-insert").action((x, c) =>
        c.copy(optimizeInsert = x)).text("Enable/Disable insert Optimzation using hive Dataframe Writer (true/false). Defaults to true.")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      help("help").text("Prints usage")

      note("###ADP Sql/Hive Query Executor Framework. ###\n")

      checkConfig(c =>
        if (c.file.isEmpty && c.sqlString.isEmpty)
          failure("Invalid input: Either file or sqlString should be provided")
        else
          success)

    }
    // parser.parse returns Option[C]
    parser.parse(args, SqlWrapperConfig()) match {
      case Some(config) => config
      case None => {
        System.exit(1)
        null // Error message would have been displayed
      }
    }

  }

}