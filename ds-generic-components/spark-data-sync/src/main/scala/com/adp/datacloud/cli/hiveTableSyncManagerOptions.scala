package com.adp.datacloud.cli

import java.util.{ Calendar, Date }

case class HiveTableSyncManagerConfig(
  @transient catalogDatabasePassword: Option[String]              = None,
  catalogJdbcUrl:                     String                      = null,
  catalogDatabaseUser:                Option[String]              = None,
  applicationName:                    String                      = s"Data Sync Util",
  parallelHiveWrites:                 Int                         = 2,
  adpInternalClients:                 String                      = s"0F18AC8F6B800272",
  tableName:                          String                      = "none",
  oracleWalletLocation:               String                      = "/app/oraclewallet",
  syncCatalogTableName:               Option[String]              = None,
  ddlReplaceMap:                      Map[String, String]         = Map(),
  ddlOnly:                            Boolean                     = false,
  syncSchemasMap:                     Option[Map[String, String]] = None,
  s3bucketName:                       String                      = "datacloud-prod-dump-us-east-1-708035784431",
  ddlFileName:                        String                      = "dataSyncDDLs.sql",
  files: Option[String]                = None) {

  def catalogSql(tableName: String = s"none"): String = {
    val baseSql = if (tableName.toLowerCase() == "none") {
      s"""
        SELECT source_table,
      	source_schema,
      	target_schema,
      	force_sync_day_of_mnth,
      	active,
      	max_hist_partitions,
      	is_view
      	FROM   ${syncCatalogTableName.get}
      	WHERE  active = 1""".stripMargin
    } else {
      val cal = Calendar.getInstance()
      cal.setTime(new Date)
      val today = cal.get(Calendar.DAY_OF_MONTH)
      s"""
  			SELECT source_table,
  			source_schema,
  			target_schema,
  			CAST ($today AS NUMBER(2)) force_sync_day_of_mnth,
  			active,
  			max_hist_partitions,
  			is_view
  			FROM   ${syncCatalogTableName.get}
  			WHERE  source_table = '$tableName'
      	AND    active = 1
      	""".stripMargin
    }
    "(" + baseSql + ")"
  }
}

object hiveTableSyncManagerOptions {

  def parse(args: Array[String]): HiveTableSyncManagerConfig = {
    val parser = new scopt.OptionParser[HiveTableSyncManagerConfig]("Data Sync Util") {

      head("Data Sync Util", this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      opt[String]('s', "s3-bucket-name").action((x, c) =>
        c.copy(s3bucketName = x)).text("s3 Bucket Name to place the DDL. defaults to datacloud-prod-dump-us-east-1-708035784431")

      opt[String]('f', "ddl-file-name").action((x, c) =>
        c.copy(ddlFileName = x)).text("name of the ddl file to be exported. defaults to dataSyncDDLs.sql")

      opt[String]('p', "password").action((x, c) =>
        c.copy(catalogDatabasePassword = Some(x))).text("Database Password")

      opt[String]('u', "user-name").action((x, c) =>
        c.copy(catalogDatabaseUser = Some(x))).text("Database User/Schema Name")

      opt[String]("oracle-wallet-location").action((x, c) =>
        c.copy(oracleWalletLocation = x)).text("Oracle wallet location. Defaults to /app/oraclewallet.")

      opt[String]('j', "jdbc-url").required().action((x, c) =>
        c.copy(catalogJdbcUrl = x)).text("Database Datasourcename (DNS)")

      opt[String]("application-name").action((x, c) =>
        c.copy(applicationName = x)).text("Name of the application.")

      opt[String]('a', "adp-internal-clients").action((x, c) =>
        c.copy(adpInternalClients = x.split(",").mkString("','"))).text("client oid of ADP internal client")

      opt[String]('c', "catalog-table-name")
        .action(
          (x, c) =>
            c.copy(
              syncCatalogTableName =
                if (x.trim.toLowerCase == "none") None
                else Some(x.trim)))
        .text("Optional Name of the catalog table to drive the sync from ")

      opt[String]('t', "table-name").action((x, c) =>
        c.copy(tableName = x.trim)).text("filter a single table for sync force it to run today.default none ")

      opt[String]('l', "sync-schema-pairs").action(
        (x, c) =>
          c.copy(
            syncSchemasMap =
              if (x.trim.toLowerCase == "none") None
              else {
                Some(x.split(",").map(_.split(":")).map(p => (p(0) -> p(1))).toMap)
              })).text("Optional source to target schema pairs csv. example dsbase:dsbaseLive,dsmain:dsmainLive,dsraw:dsrawLive")

      opt[String]('r', "ddl-replacements").action(
        (x, c) =>
          c.copy(
            ddlReplaceMap =
              x.split(",").map(_.split("=")).map(p => (p(0) -> p(1))).toMap)).text("Optional list of string replacements to final ddls. example prd-ds-live=fit-ds-live,942420684378=591853665740")

      opt[Boolean]("ddl-only").action((x, c) => c.copy(ddlOnly = x)).text("boolean value to skip sync and generate ddls only. default false")

      opt[Int]('p', "parallel-hive-writes").action((x, c) =>
        c.copy(parallelHiveWrites = x)).text("Number of hive writes in parallel. Default 2")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      checkConfig(
        c =>
          if (c.catalogDatabaseUser.isDefined != c.catalogDatabasePassword.isDefined)
            failure(
            "--password and --user-name should be specified or omitted together")
          else if (c.syncSchemasMap.isDefined == c.syncCatalogTableName.isDefined)
            failure(
            "one among --sync-schema-pairs and --catalog-table-name is mandatory but both cannot be specified.")
          else
            success)

    }
    parser.parse(args, HiveTableSyncManagerConfig()).get
  }

}