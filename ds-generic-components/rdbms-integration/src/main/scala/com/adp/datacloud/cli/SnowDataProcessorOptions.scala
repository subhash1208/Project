package com.adp.datacloud.cli

import scala.io.Source
import org.apache.spark.SparkFiles
import com.adp.datacloud.ds.aws.ParameterStore
import com.adp.datacloud.ds.aws.SecretsStore
import oracle.jdbc.OracleConnection

case class SnowDataProcessConfig(
    dxrJdbcUrl: String = null,
    processMode: String  = "export",
    targetEnv:   String = null,
    account: Option[String] = None,
    user: Option[String] = None,
    role: Option[String] = None,
    warehouse: Option[String] = None,
    database: String = null,
    schema: String = null,
    sqlParams: List[String]   = List(),
    targetTbl: String = null,
    preFile: Option[String] = None,    // pre action sqls to be executed at datalake/ snowflake
    postFile: Option[String] = None,   // post actions sqls to be executed at datalake/ snowflake
    sqlFile: Option[String] = None,    // used as sql to read from datalake or snowflake
    inputParams:   List[String]   = List(),
    tableName: Option[String] = None,
    columnMapping: String = "order", 
    useStaging: String = "on",      // default for ingestion
    ovrKey : Option[String] = None,
    partitionSizeMB: String = "100", // default for ingestion helps creation multiple micropartitions
    files: Option[String]                = None
    ) {

def appName = "Snowflake_process_"+processMode+"_"+targetTbl
def url = account.get+".snowflakecomputing.com"
def password = {SecretsStore.getCredential("__SF_SECRET__", user.get)}

lazy val jdbcProperties = Map(
      "url" -> dxrJdbcUrl,
      "driver" -> "oracle.jdbc.driver.OracleDriver",
      OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION -> "/app/oraclewallet" )



def sql(file: Option[String]) = {
    val source = file match {
      
      case Some(x) => {

    (if (new java.io.File(x).exists()) {
          // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
          Source.fromFile(x)("UTF-8")
        } else {
          Source.fromFile(SparkFiles.get(x))("UTF-8")
        }).getLines().map { _.replaceAll("^\\s*\\-\\-.*$", "").replaceAll("^\\s*$", "") }.filter(!_.isEmpty()).mkString("\n")    }
      
      case None => null
}
   val hiveconfsrc = inputParams.foldLeft(source) { (y, x) =>
      ("\\$\\{hiveconf:" + x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))}.replaceAll("\\$\\{hiveconf:[a-zA-Z0-9]+\\}", "")

    sqlParams.foldLeft(hiveconfsrc) { (y, x) =>
      ("\\$\\{sqlconf:" + x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))}.replaceAll("\\$\\{sqlconf:[a-zA-Z0-9]+\\}", "").trim
}


}
object SnowDataProcessorOptions {

     def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[SnowDataProcessConfig]("Snowflake data processor") {
      head("Snowflake data processor", this.getClass.getPackage().getImplementationVersion())

      override def showUsageOnError = true
      
      opt[String]('z', "dxr-jdbc-url").required.action((x, c) =>
        c.copy(dxrJdbcUrl = x)).text("DXR credentials for Logging")
        
      opt[String]('m', "process-mode").action((x, c) =>
        c.copy(processMode = x)).text("Process mode for snowflake options: export, extract")
        
      opt[String]('h', "target-env").action((x, c) =>
        c.copy(targetEnv = x)).required.text("Target environment for export/extract. Eg: DIT, FIT, IPE, IAT, UAT, PROD. Required if credentials are not given")
        
      opt[String]('l', "account").action((x, c) =>
        c.copy(account = Some(x))).text("Account details")
        
      opt[String]('u', "user").action((x, c) =>
        c.copy(user = Some(x))).text("Snowflake db UserName")
        
      opt[String]('r', "role").action((x, c) =>
        c.copy(role = Some(x))).text("Role")
      
      opt[String]('w', "warehouse").action((x, c) =>
        c.copy(warehouse = Some(x))).text("Snowflake warehouse specifier")

      opt[String]('d', "database").required.action((x, c) =>
        c.copy(database = x)).text("Snowflake database specifier")
        
      opt[String]('s', "schema").required.action((x, c) =>
        c.copy(schema = x)).text("Snowflake schema specifier")
           
      opt[String]('n', "target-table").required.action((x, c) =>
        c.copy(targetTbl = x)).text("Snowflake target table name or comma separated target table names")
               
      opt[String]('f', "sqlfile").action((x, c) =>
        c.copy(sqlFile = Some(x))).text("Input Sql file or multiple sql semicolon separated in the sqeuence of target table provided")
      
      opt[String]('x', "pre-sql").action((x, c) =>
        c.copy(preFile = Some(x))).text("Input pre actions")
      
      opt[String]('y', "post-sql").action((x, c) =>
        c.copy(postFile = Some(x))).text("Input post actions")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")
        
      opt[String]('t', "table-name").action((x, c) =>
        c.copy(tableName = Some(x))).text("Table name for export/extract, or multiple table names of comma separated tables in the sequence of target table provided")

      opt[String]('p', "hiveconf").unbounded().action((x, c) =>
        c.copy(inputParams = c.inputParams ++ List(x))).text("Hiveconf variables to replaced in query")
      
      opt[String]('q', "sqlconf").unbounded().action((x, c) =>
        c.copy(sqlParams = c.sqlParams ++ List(x))).text("Hiveconf variables to replaced in query")
        
      opt[String]('c', "col-mapping").action((x, c) =>
        c.copy(columnMapping = x)).text("Column mapping approach order/name. Defaulted to order")
        
      opt[String]('j', "use-stage").action((x, c) =>
        c.copy(useStaging = x)).text("Use staging table values on/off. Default on")
      
      opt[String]('o', "ovr-ky").action((x, c) =>
        c.copy(ovrKey = Some(x))).text("Overwrite key to truncate the data from the same key")
        
      opt[String]('b', "partition-size").action((x, c) =>
        c.copy(partitionSizeMB = x)).text("Value for partition size in MB")

      help("help").text("Prints usage")

      note("###ADP Snowflake Processing Framework. ###\n")

      checkConfig(c =>
       
        if (c.sqlFile.isEmpty && 
            c.tableName.isEmpty)
          failure("Invalid input: Either sql or table name should be provided")
          
        else if (c.user.isEmpty || 
                 c.account.isEmpty || 
                 c.warehouse.isEmpty || 
                 c.role.isEmpty)
            failure("Either user credentials (user, account, warehouse, role, password ) or target-env should be provided.")

        else
            success)
    }
    args.foreach(println)
    parser.parse(args, SnowDataProcessConfig()).get
    
  }

}