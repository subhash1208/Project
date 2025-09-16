package com.adp.datacloud.cli

case class VersionedTablesPatcherConfig(
    databases: List[String]  = List(),
    tableNames: List[String] = List(),
    depth: Int               = 1,
    files: Option[String]    = None) {
  def applicationName = {
    s"Versioned Tables Patcher ${databases.mkString}"
  }
}

object VersionedTablesPatcherOptions {
  def parse(args: Array[String]): VersionedTablesPatcherConfig = {
    val parser =
      new scopt.OptionParser[VersionedTablesPatcherConfig]("Versioned Tables Patcher") {

        head(
          "Versioned Tables Patcher",
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

        opt[Int]('n', "depth").action((x, c) => c.copy(depth = x))

        opt[String]("files")
          .action((x, c) => c.copy(files = Some(x)))
          .text("csv string dependent files with absolute paths")

        checkConfig(
          c =>
            if (c.databases.isEmpty)
              failure("--d or --database-name should not be empty.")
            else if (c.tableNames.isEmpty)
              failure("--t or --table-names should not be empty.")
            else
              success)
      }
    parser.parse(args, VersionedTablesPatcherConfig()).get
  }

}
