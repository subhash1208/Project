package com.adp.datacloud.cli

case class ParallelSqlConfig(
  connFile:           String              = null,
  sqlFile:            String              = null,
  logFile:            String              = null,
  jdbcPrefix:         String              = "jdbc:oracle:thin", // Defaults to oracle connection.
  sourceDatabasePort: Integer             = 1521,
  maxParallelThreads: Integer             = 5,
  inputParams:        Map[String, String] = Map(),
  applicationName:    String              = "Parallel Sql Executor") {
}
object parallelSqlExecutorOptions {
  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[ParallelSqlConfig]("Parallel Sql Executor") {
      head("ParallelSqlExecutor", this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      opt[String]('c', "connection-file").required().action((x, c) =>
        c.copy(connFile = x)).text("Schema connections file")

      opt[String]('n', "application-name").action((x, c) =>
        c.copy(applicationName = x)).text("Name of the application.")

      opt[String]('s', "sql-file").required().action((x, c) =>
        c.copy(sqlFile = x)).text("Sql File to be Executed")

      opt[String]('o', "log-file").required().action((x, c) =>
        c.copy(logFile = x)).text("log File to be Executed")

      opt[Int]("source-port").action((x, c) =>
        c.copy(sourceDatabasePort = x)).text("Database Port. Defaults to 1521")

      opt[Int]('m', "max-parallel-threads").action((x, c) =>
        c.copy(maxParallelThreads = x)).text("Max Number of Parallel connections. Defaults to 5")

      opt[String]("sql-conf").unbounded().action((x, c) =>
        c.copy(inputParams = c.inputParams + (x.split("=")(0) -> x.split("=")(1)))).text("Hiveconf variables to replaced in query")

      //TODO: Create options for customizing the other parameters

      help("help").text("Prints usage")

      note("ADP Spark Based Parallel Sql Executor. \n")
    }

    // parser.parse returns Option[C]
    parser.parse(args, ParallelSqlConfig()).get

  }

}