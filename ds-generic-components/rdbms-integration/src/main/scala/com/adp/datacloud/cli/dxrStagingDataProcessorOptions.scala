package com.adp.datacloud.cli

import com.adp.datacloud.ds.util.DXRLoggerHelper

case class dxrStagingDataProcessorConfig(
    applicationName: String                  = "Rerun DXR Hive Write",
    skipRowCountValidation: Boolean          = false,
    skipStagingCleanup: Boolean              = false,
    dxrJdbcUrl: String                       = null,
    esUsername: Option[String]               = None,
    esPwd: Option[String]                    = None,
    oracleWalletLocation: String             = "/app/oraclewallet",
    addonDistributionColumns: Option[String] = None,
    applicationId: String                    = null,
    files: Option[String]                    = None,
    recoverMode: Boolean                     = false)
    extends DxrBaseConfig {

  override def dxrProductName: String = ""

  override def jdbcDBType: String = "oracle"

  override def inputParams: Map[String, String] = Map()

  override def dxrProductVersion: Option[String] = None

  //get row with most recent attemptId
  lazy val jobConfigSql =
    s"""
        (SELECT
           application_id,
           cast(attempt_id as CHAR) as attempt_id,
           application_name,
           product_name,
           dxr_sql,
           input_sql,
           job_config,
           job_status,
           job_status_code,
           msg,
           parent_application_id,
           cluster_id,
           workflow_name,
           state_machine_execution_id,
           env,
           job_start_time,
           job_end_time,
           is_error,
           log_date
      FROM
          ${DXRLoggerHelper.extractStatusTable}
      WHERE
          application_id = '$applicationId'
          order by attempt_id desc,log_date desc)
      """

  lazy val jobLogsSql =
    s"""
        (select * from ${DXRLoggerHelper.extractLogsTable} where application_id = '$applicationId' and is_error = 1 )
      """.trim
}

object dxrStagingDataProcessorOptions {
  private val parser =
    new scopt.OptionParser[dxrStagingDataProcessorConfig]("rerun dxr hive write") {
      head("rerunDxrHiveWriteConfig", this.getClass.getPackage.getImplementationVersion)

      override def showUsageOnError = true

      //required fields

      opt[String]('h', "dxr-jdbc-url")
        .required()
        .action((x, c) => c.copy(dxrJdbcUrl = x))
        .text("Database Datasourcename (DNS)")

      opt[String]('a', "application-id")
        .required()
        .action((x, c) => c.copy(applicationId = x))
        .text("applicationId of previous job that failed while writing to hive")

      //optional fields

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      opt[String]("oracle-wallet-location")
        .action((x, c) => c.copy(oracleWalletLocation = x))
        .text("Oracle wallet location. Defaults to /app/oraclewallet.")

      opt[String]('n', "application-name")
        .action((x, c) => c.copy(applicationName = x))
        .text("Name of the application.")

      opt[Boolean]("skip-rowcount-validation")
        .action((x, c) => c.copy(skipRowCountValidation = x))
        .text("skip staging rowcount validation. Default false")

      opt[Boolean]("skip-staging-cleanup")
        .action((x, c) => c.copy(skipStagingCleanup = x))
        .text(
          "skip cleaning up the staging files. Useful for performance testing writes. Default false")

      opt[Boolean]("recovery-mode")
        .action((x, c) => c.copy(recoverMode = x))
        .text(
          "boolean flag to switch between rerunning failed connections to recovering data from staging directory. Default false")

      opt[String]('t', "addon-distribution-columns")
        .action((x, c) => c.copy(addonDistributionColumns = Some(x)))
        .text("This needs to be passed as we will not do schema scan")

      opt[String]("es-username")
        .action((x, c) => c.copy(esUsername = Some(x)))
        .text("Elasticsearch username")

      opt[String]("es-password")
        .action((x, c) => c.copy(esPwd = Some(x)))
        .text("Elasticsearch password. Not secure and hence not recommended. By default, password shall be lookedup from Secretstore")

      help("help").text("Prints usage")

      note("rerun dxr hive write. \n")
    }

  def parse(args: Array[String]): dxrStagingDataProcessorConfig = {
    parser.parse(args, dxrStagingDataProcessorConfig()).get
  }
}
