package com.adp.datacloud.cli

case class ExportConfig(
  file:                String         = null,
  exportDatabaseName:  String         = null,
  exportDatabaseTable: String         = null,
  filterSpec:          Option[String] = None,
  targetTable:         String         = null,
  jdbcPrefix:          String         = "jdbc:oracle:thin", // Defaults to oracle connection.
  sourceDatabasePort:  Integer        = 1521,
  maxParallelThreads:  Integer        = 5,
  connectionIdColumn:  String         = "db_schema",
  applicationName:     String         = "Parallel RDBMS Data Exporter") {

}

object dataExportOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[ExportConfig]("Parallel RDBMS Data Exporter") {
      head("ParallelRdbmsDataExporter", this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      opt[String]('c', "input").required().action((x, c) =>
        c.copy(file = x)).text("Schema connections file")

      opt[String]('n', "application-name").action((x, c) =>
        c.copy(applicationName = x)).text("Name of the application.")

      opt[Int]("source-port").action((x, c) =>
        c.copy(sourceDatabasePort = x)).text("Database Port. Defaults to 1521")

      opt[String]('d', "export-hive-db").required().action((x, c) =>
        c.copy(exportDatabaseName = x)).text("Export Hive Database Name")

      opt[String]('h', "export-hive-table").required().action((x, c) =>
        c.copy(exportDatabaseTable = x)).text("Export Hive Table Name")

      opt[String]("connection-id-column").required().action((x, c) =>
        c.copy(connectionIdColumn = x)).text("Column containing connection identifier. Defaults to 'db_schema'")

      opt[String]('p', "filter-spec").action((x, c) =>
        c.copy(filterSpec = Some(x))).text("Comma separated Export filter Spec (column1=value1,column2=value2). Optional example ENV=DIT")

      opt[String]('t', "target-warehouse-table").required().action((x, c) =>
        c.copy(targetTable = x)).text("Target warehouse table to which we need to export data")

      opt[Int]('m', "max-parallel-threads").action((x, c) =>
        c.copy(maxParallelThreads = x)).text("Max Number of Parallel connections. Defaults to 5")

      //TODO: Create options for customizing the other parameters

      help("help").text("Prints usage")

      note("ADP Spark Based Parallel RDBS Data Exporter. \n")
    }

    // parser.parse returns Option[C]
    parser.parse(args, ExportConfig()).get

  }

}