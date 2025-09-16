package com.adp.datacloud.cli

case class HiveDDLGeneratorConfig(
  dbName:          String         = null,
  s3BucketPath:    String         = null,
  applicationName: String         = s"Hive DDLs Generator",
  glueDbName:      Option[String] = None,
  ddlFileName:     String         = null,
  files: Option[String]                = None) {

  def getGlueDbName(): String = {
    if (glueDbName.isDefined) {
      glueDbName.get
    } else {
      s3BucketPath.split("/").last
    }
  }
}

object hiveDDLGeneratorOptions {

  def parse(args: Array[String]): HiveDDLGeneratorConfig = {
    val parser = new scopt.OptionParser[HiveDDLGeneratorConfig]("Hive Tables DDL Generator") {

      head("Hive DDL Generator", this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      opt[String]('d', "dbname").required().action((x, c) =>
        c.copy(dbName = x)).text("Database Name")

      opt[String]('s', "s3-bucket-path").required().action((x, c) =>
        c.copy(s3BucketPath = x)).text("s3 Bucket path to replace in DDL")

      opt[String]('f', "ddl-file-name").required().action((x, c) =>
        c.copy(ddlFileName = x)).text("name of the ddl file to be exported.")

      opt[String]('g', "gluedb-name").action((x, c) =>
        c.copy(glueDbName = Some(x))).text("taget dbName in glue metastore. Optional")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")
    }
    parser.parse(args, HiveDDLGeneratorConfig()).get
  }

}