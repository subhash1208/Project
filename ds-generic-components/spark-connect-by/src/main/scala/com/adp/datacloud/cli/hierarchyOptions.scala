package com.adp.datacloud.cli

import org.apache.spark.SparkFiles

import scala.io.Source

/**
 * Hierarchy Computation Framework Command Line Options utility class.
 */
case class HierarchyConfig(
    input: String                        = null,
    pkColumnName: String                 = "pers_obj_id",
    fkColumnName: String                 = "mngr_pers_obj_id",
    groupColumns: List[String]           = List(),
    outputDBName: String                 = "default",
    outputHierarchyTable: String         = null,
    outputExplodedHierarchyTable: String = null,
    outputPartitionSpec: Option[String]  = None,
    rootIndicatorColumn: Option[String]  = None,
    inputParams: List[String]            = List(),
    enableCheckpoint: Boolean            = false,
    distributionSpec: Option[String]     = None,
    applicationName: String              = "ADP Spark Hierarchy Computation Framework",
    saveFormat: String                   = "parquet",
    multiColumnSpec: List[String]        = List(),
    files: Option[String]                = None) {

  def sql: String = {

    val sql = if (input.endsWith(".sql")) {

      val sourceFile = if (new java.io.File(input).exists()) {
        // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
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

  def startWithRowsExpression = {
    rootIndicatorColumn match {
      case Some(x) => x
      case None =>
        fkColumnName + " IS null OR " + fkColumnName + " = 'ONE' OR " + fkColumnName + " = " + pkColumnName + " OR " + fkColumnName + " = ''"
    }
  }

}

object hierarchyOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[HierarchyConfig]("Hierarchy Builder") {
      head("HierarchyBuilder", "v3.0")

      override def showUsageOnError = true

      opt[String]('n', "application-name")
        .action((x, c) => c.copy(applicationName = x))
        .text("Name of the application.")

      opt[String]('i', "input-spec")
        .required()
        .action((x, c) => c.copy(input = x))
        .text("Input Either of 1) Fully qualified table or 2) Sql file")

      opt[String]('g', "group-columns")
        .required()
        .action((x, c) => c.copy(groupColumns = x.split(",").toList))
        .text("Comma Separated Group/partition columns")

      opt[String]('o', "output-db-name")
        .required()
        .action((x, c) => c.copy(outputDBName = x))
        .text("Hive Database name where output tables should be saved. Ignored if hdfs-output-path is provided")

      opt[String]("connect-by-column")
        .action((x, c) => c.copy(pkColumnName = x))
        .text("Primary key for hierarchical join. Defaults to pers_obj_id")

      opt[String]("connect-by-parent")
        .action((x, c) => c.copy(fkColumnName = x))
        .text("Primary key for hierarchical join. Defaults to mngr_pers_obj_id")

      opt[String]("root-indicator-column")
        .action((x, c) => c.copy(rootIndicatorColumn = Some(x)))
        .text("Column with boolean data-type to indicate the rows which should be used as root. If not specified, default start-rows logic is applied.")

      opt[String]("hierarchy-table")
        .required()
        .action((x, c) => c.copy(outputHierarchyTable = x))
        .text("Hive Database table name where hierarchy computation results are stored (contains direct_children etc...)")

      opt[String]("hierarchy-exploded-table")
        .required()
        .action((x, c) => c.copy(outputExplodedHierarchyTable = x))
        .text("Hive Database table name where hierarchy exploded results are stored (contains level_from_parent field)")

      opt[String]('t', "output-partition-spec")
        .action((x, c) => c.copy(outputPartitionSpec = Some(x)))
        .text("Output Partition Spec (column=value). Optional. Example pp=2017q1")

      opt[String]('m', "multi-column-spec")
        .unbounded()
        .action((x, c) => // --multi-column-spec yr=2015,2016,2017
          c.copy(multiColumnSpec = c.multiColumnSpec ++ List(x)))
        .text("Partition Spec")

      opt[String]("hiveconf")
        .unbounded()
        .action((x, c) => c.copy(inputParams = c.inputParams ++ List(x)))
        .text("Hiveconf variables to replaced in input")

      opt[String]('d', "distribute-by")
        .action((x, c) => c.copy(distributionSpec = Some(x)))
        .text("Distribute input by the specified columns prior to aggregate processing.")

      opt[Boolean]('c', "enable-checkpoint")
        .action((x, c) => c.copy(enableCheckpoint = x))
        .text(
          "Persist and Checkpoint the input dataframe prior to aggregate processing. Defaults to false.")

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

      note("ADP Hierarchy Builder Framework. \n")

    }

    // parser.parse returns Option[C]
    parser.parse(args, HierarchyConfig()).get

  }

}
