package com.adp.datacloud.cli

case class TableStatsGeneratorConfig(
  inputSchemas:         List[String]   = List(),
  filterConditions:     List[String]   = List(),
  outputDBJdbcURL:      String         = null,
  oracleWalletLocation: Option[String] = None,
  outputDBUserName:     Option[String] = None,
  outputDBPassword:     Option[String] = None,
  overWriteCounts:      Boolean        = false,
  applicationName:      String         = "TableStatsGenerator",
  files: Option[String]                = None) {

}

object tableStatsGeneratorOptions {

  def parse(args: Array[String]): TableStatsGeneratorConfig = {
    val parser = new scopt.OptionParser[TableStatsGeneratorConfig]("Table Stats Generator") {

      head("Table Stats Generator", this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      opt[String]('i', "input-schemas").required().action((x, c) =>
        c.copy(inputSchemas = x.split(",").toList)).text("Input Database Names")

      opt[String]('f', "filter-conditions").action((x, c) =>
        c.copy(filterConditions = x.split(",").toList)).text("filter Conditions")

      opt[String]('a', "application-name").action((x, c) =>
        c.copy(applicationName = x)).text("Name of the application.")

      opt[String]('j', "jdbc-url").action((x, c) =>
        c.copy(outputDBJdbcURL = x)).text("Output db jdbc url.")

      opt[String]('w', "oracle-wallet").action((x, c) =>
        c.copy(oracleWalletLocation = Some(x))).text("oracle wallet location.")

      opt[String]('u', "db-user-name").action((x, c) =>
        c.copy(outputDBUserName = Some(x))).text("output db username.")

      opt[String]('p', "db-password").action((x, c) =>
        c.copy(outputDBPassword = Some(x))).text("output db password.")

      opt[Boolean]('y', "overwrite-counts").action((x, c) =>
        c.copy(overWriteCounts = x)).text("truncate previous counts and regenerate")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

    }
    parser.parse(args, TableStatsGeneratorConfig()).get
  }

}