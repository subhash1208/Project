package com.adp.datacloud.cli

case class SparkDDLGeneratorConfig(
  applicationName: String              = s"Hive To Spark DDL Migrator",
  dbNames:         List[String]        = List(),
  tableNames:      List[String]        = List(),
  s3bucketName:    String              = "datacloud-prod-dump-us-east-1-708035784431",
  sqlPrefix:       String              = s"default",
  replaceMap:      Map[String, String] = Map(),
  files: Option[String]                = None)

object sparkDDLGeneratorOptions {

  def parse(args: Array[String]): SparkDDLGeneratorConfig = {

    val parser = new scopt.OptionParser[SparkDDLGeneratorConfig](
      "Hive to Spark DDL migrator") {

      head(
        "Hive to Spark DDL migrator",
        this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      opt[String]("application-name")
        .action((x, c) => c.copy(applicationName = x))
        .text("Name of the application.")

      opt[String]('d', "db-names")
        .required()
        .action((x, c) =>
          c.copy(
            dbNames = c.dbNames ++ x.split(",").map(_.trim)))
        .text("glue db schema names csv")

      opt[String]('t', "table-names")
        .required()
        .action((x, c) =>
          c.copy(
            tableNames = c.tableNames ++ x.split(",").map(_.trim)))
        .text("full table names csv")

      opt[String]('s', "s3-bucket-name")
        .action((x, c) => c.copy(s3bucketName = x))
        .text(
          "s3 Bucket Name to place the DDL. defaults to datacloud-prod-dump-us-east-1-708035784431")

      opt[String]('p', "sql-file-prefix")
        .action((x, c) => c.copy(sqlPrefix = x))
        .text("prefix for the sqls to be uploaded to s3.")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      opt[String]('r', "ddl-replacements")
        .action(
          (x, c) =>
            c.copy(
              replaceMap =
                c.replaceMap ++ x
                  .split(",")
                  .map(_.split("="))
                  .map(p => p(0) -> p(1))
                  .toMap))
        .text(
          "Optional csv list a replaceble values. example dit=fit,prod=live")

    }
    parser.parse(args, SparkDDLGeneratorConfig()).get

  }

}

