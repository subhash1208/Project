package com.adp.datacloud.cli

case class DropTableUtilConfig(
    databases: List[String]  = List(),
    tableNames: List[String] = List()) {
  def applicationName = {
    s"Drop Tables in ${databases.mkString}"
  }
}

object DropTableUtilOptions {
  def parse(args: Array[String]): DropTableUtilConfig = {
    val parser =
      new scopt.OptionParser[DropTableUtilConfig]("Drop Table Util") {

        head(
          "Utility to drop plain and Versioned Tables",
          this.getClass.getPackage.getImplementationVersion)

        override def showUsageOnError = true

        opt[String]('d', "database-names")
          .required()
          .action((x, c) =>
            c.copy(databases =
              c.databases ++ x.split(",").map(_.trim).filter(_.nonEmpty)))
          .text("Database Name")

        opt[String]('t', "table-names")
          .required()
          .action((x, c) =>
            c.copy(tableNames =
              c.tableNames ++ x.split(",").map(_.trim).filter(_.nonEmpty)))
          .text("table names csv")

        checkConfig(
          c =>
            if (c.databases.isEmpty)
              failure("--d or --database-name should not be empty.")
            else if (c.tableNames.isEmpty)
              failure("--t or --table-names should not be empty.")
            else
              success)
      }
    parser.parse(args, DropTableUtilConfig()).get
  }

}
