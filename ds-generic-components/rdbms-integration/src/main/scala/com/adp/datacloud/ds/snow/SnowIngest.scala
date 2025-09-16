package com.adp.datacloud.ds.snow

import org.apache.log4j.Logger
import java.io._
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.{ lit, col, max, expr, coalesce }
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.Utils
import com.adp.datacloud.cli.SnowDataProcessConfig
import com.adp.datacloud.ds.util.{StepLogs, SnowProcessLogger}

/*
#	This class is responsible for exporting data to the specified snowflake account.
#	This whole program does not take care of the whole environment setup. Connectivity should be configured before running this program.
# The credentials are read from configuration. Passwords will be retrieved from secret store.
# Program is broken to different modules. pre-actions, overwrite-handling, export, post-actions and log-feed.
# Program is currently writing the logs to DXR catalog db. Separate logs will be available for each step mentioned above further broken to individual steps.
# Function ingest() is the main handler. Other small functions used as utilities are verifyLogs(), sqlActions(), executeSnowqSql() and zip3List()
# Main executions are wrapped around 'try' to log the issues and exit gracefully.
# Config related issues will not be logged in DXR. The failures should be instant leaving logs in cluster only
*/

class SnowIngest(config : SnowDataProcessConfig)(implicit sparkSession: SparkSession) {

private val logger = Logger.getLogger(getClass)

def verifyLogs(logs: List[StepLogs], msg: String, config: SnowDataProcessConfig) = {
  
  if (logs.filter(x => x.STATUS.equals("FAIL")).size > 0)  
  {     
    SnowProcessLogger(logs,config)
    println(msg)
    logger.error(msg)
    throw new Exception(msg)
  } 
}

def zip3List(x:List[String],
             y:List[String],
             z:List[String]): List[(String,String,String)] = for (((x,y),z) <- x zip y zip z) yield (x,y,z)
 
def sqlActions(actionType: String, 
               options: Map[String,String], 
               sqls : String): List[StepLogs] = {
    val logs = for (sql <- sqls.split(";"))  yield executeSnowqSql(actionType, options, sql)
    logs.toList  }

def executeSnowqSql(actionType: String, 
                    options: Map[String,String], 
                    sql: String): StepLogs = {
    val startTime  = System.currentTimeMillis
  try{
      println(s"Running Snowflake query : ${sql}")
      logger.info(s"Running Snowflake query : ${sql}")
      Utils.runQuery(options,sql)
      StepLogs(this.sparkSession.sparkContext.applicationId,
               actionType,
               sql,
               (System.currentTimeMillis - startTime),
              "PASS")
  }
  catch {
      case ex: Exception => { println(s"Error running query :  ${ex}")
                              logger.error(s"Error running query :  ${ex}")
                              println(sql)
                              logger.error(sql)
                              ex.printStackTrace
                              val sw = new StringWriter
                              ex.printStackTrace(new PrintWriter(sw))
                              StepLogs(this.sparkSession.sparkContext.applicationId,
                                       actionType,
                                       sql,
                                       (System.currentTimeMillis - startTime),
                                      "FAIL",
                                       sw.toString)}
    }
}

def ingest(): List[StepLogs] = {
  val secret = this.config.password match {  case Some(x) => {logger.info("Password retrival successful.")
                                                              x    }  
                                             case None => {  println("Password could not be retrieved.")
                                                             logger.error("Password could not be retrieved.")
                                                             throw new Exception
                                                             null    }}
  // Options for connecting to snowflake
  val sfOptions = Map(
        "sfURL" -> this.config.url,
        "sfAccount" -> this.config.account.get,
        "sfUser" -> this.config.user.get,
        "sfPassword" -> secret,
        "sfDatabase" -> this.config.database,
        "sfSchema" -> this.config.schema,
        "sfWarehouse" -> this.config.warehouse.get,
        "sfRole" -> this.config.role.get, 
        "column_mapping" -> this.config.columnMapping,
        "usestagingtable" -> this.config.useStaging,
        "partition_size_in_mb" -> this.config.partitionSizeMB)
  
  val srcGroup = config.tableName match {
    case None     => config.sql(config.sqlFile)
                           .split(";")
                           .toList
                           
    case Some(x)  => x.split(",")
                      .toList
    }
  
   val tgtGroup = config.targetTbl.split(",").toList
   
   // Extracting overwrite key values from source dataframe
   val ovrKyValGroup = config.ovrKey match {
     case Some(key) => config.tableName match {
                     case None => srcGroup.map(x => sparkSession.sql(s"${x}")
                                             .select(col(key).cast("string"))
                                             .distinct
                                             .collect
                                             .map(_.getString(0))
                                             .map(m => "'"+m+"'")
                                             .mkString(",")) 
                                             
                     case Some(x) => srcGroup.map(x => sparkSession.table(s"${x}")
                                             .select(col(key).cast("string"))
                                             .distinct
                                             .collect
                                             .map(_.getString(0))
                                             .map(m => "'"+m+"'")
                                             .mkString(","))  }
     case None => srcGroup.map(x => "null")
   }
   
   // creating a tuple from source sql/table, targets and overwrite key values 
   val srcTgtGroup = (srcGroup.size.==(tgtGroup.size) && srcGroup.size.==(ovrKyValGroup.size)) match {
     case true => zip3List(srcGroup,tgtGroup,ovrKyValGroup)
     case false => {
                     println("Source table/sql are not the same as number of targets specified. Aborting ingest..!!")
                     logger.error("Source table/sql are not the same as number of targets specified. Aborting ingest..!!")
                     throw new Exception("Source table/sqls are not the same as number of targets specified. Aborting ingest..!!")
                   }}
   
   // Performing pre-actions and collecting logs
   val preActionLogs = config.preFile match { 
    case Some(x) => {  val logs = sqlActions("PREACTION",sfOptions,srcTgtGroup.foldLeft(config.sql(Some(x)) )((a,b) => a.replaceAll("\\$\\{sqlconf:"+s"${b._2}"+"_ovr_vals\\}",b._3)))  //Providing overwrite key values with the specific target tables
                       println("Preactions executed.")
                       logger.info("Preactions executed.")
                       logs    }
    case None    => {  println("No Preactions provided.")
                       logger.warn("No Preactions provided.")
                       List()  }  }
   
   verifyLogs(preActionLogs, "Failures in PreSteps, Please check logs.", this.config)
   
   // Handling overwriting. Fetching overwrite key values and making sure the older records are purged before pushing files.
   val overwriteHandleLogs = config.ovrKey match {
                 case Some(key) => {
                                     for (srcTgt <- srcTgtGroup ) yield { 
                                         println(srcTgt)
                                         logger.info(srcTgt)
                                         executeSnowqSql("OVERWRITE_HANDLE",sfOptions,s"delete from ${srcTgt._2} where ${key} in (${srcTgt._3})") }  }
                 case None      => {  println("No overwrite key provided.")
                                      logger.warn("No overwrite key provided.")
                                      List() } }
 
   verifyLogs(overwriteHandleLogs ++ preActionLogs , "Failures in Overwrite Handling, Please check logs.", this.config)
 
   // Writing to Snowflake. Limited to use append mode as overwrite mode drops the table in snowflake. Overwrite handling used for handling overwrite for specific overwrite key values.
   val exportLogs = for (srcTgt <- srcTgtGroup ) yield {
   config.tableName match {
   case None  => { val startTime = System.currentTimeMillis
                   try { 
                         println(srcTgt)
                         logger.info(srcTgt)
                         sparkSession.sql(s"${srcTgt._1}")
                                     .write
                                     .format(SNOWFLAKE_SOURCE_NAME)
                                     .options(sfOptions)
                                     .option("dbtable", srcTgt._2)
                                     .mode(SaveMode.Append)
                                     .save
                          logger.info(s"Writing to ${srcTgt._2} successful.")
                          StepLogs(this.sparkSession.sparkContext.applicationId,
                                   "EXPORT",
                                   s"export : ${srcTgt._1} to ${srcTgt._2}",
                                   (System.currentTimeMillis - startTime),
                                   "PASS")
                         }  
                   catch {
                     case ex:Exception => {  logger.error("Caught exception while writing to snowflake") 
                                             ex.printStackTrace
                                             val sw = new StringWriter
                                             ex.printStackTrace(new PrintWriter(sw))
                                             StepLogs(this.sparkSession.sparkContext.applicationId,
                                                      "EXPORT",
                                                     s"export : ${srcTgt._1} to ${srcTgt._2}",
                                                     (System.currentTimeMillis - startTime),
                                                      "FAIL",
                                                      sw.toString)
                                           }
      }                
   }                            
   case Some(x) =>{ val startTime = System.currentTimeMillis
     try {
                    println(srcTgt)
                    logger.info(srcTgt)
                    sparkSession.table(s"${srcTgt._1}")
                                .write
                                .format(SNOWFLAKE_SOURCE_NAME)
                                .options(sfOptions)
                                .option("dbtable", srcTgt._2)
                                .mode(SaveMode.Append)
                                .save 
                    logger.info(s"Writing to ${srcTgt._2} successful.")
                    StepLogs(this.sparkSession.sparkContext.applicationId,
                            "EXPORT",
                            s"export : ${srcTgt._1} to ${srcTgt._2}",
                            (System.currentTimeMillis - startTime),
                            "PASS")
                         }
   catch {  
     case ex:Exception => {  logger.error("Caught exception while writing to snowflake") 
                                             ex.printStackTrace
                                             val sw = new StringWriter
                                             ex.printStackTrace(new PrintWriter(sw))
                                             StepLogs(this.sparkSession.sparkContext.applicationId,
                                                      "EXPORT",
                                                     s"export : ${srcTgt._1} to ${srcTgt._2}",
                                                     (System.currentTimeMillis - startTime),
                                                      "FAIL",
                                                      sw.toString)
                           }
      }  
    }
  } 
}
  
  //Verifying accumulated logs. If found a failure then write to DXR log table and exit the program.
  verifyLogs(exportLogs ++ overwriteHandleLogs ++ preActionLogs, "Failures while exporting Data, Please check logs.", this.config)
 
  // post write actions
  val postActionLogs = 
       config.postFile match{
              case Some(x) => {  val logs = sqlActions("POSTACTION", sfOptions, srcTgtGroup.foldLeft(config.sql(Some(x)) )((a,b) => a.replaceAll("\\$\\{sqlconf:"+s"${b._2}"+"_ovr_vals\\}",b._3)))
                                 println("Postactions executed.")
                                 logger.info("Postactions executed.")
                                 logs  }
              case None    => {  println("No Postactions provided.")
                                 logger.warn("No Postactions provided.")
                                 List() }
  }  

  verifyLogs(postActionLogs ++ exportLogs ++ overwriteHandleLogs ++ preActionLogs, "Failures in postActions, Please check logs.", this.config)
  //Returning all logs if the job is success. This will write all step logs and exit the program without exception.
  preActionLogs ++ overwriteHandleLogs ++ exportLogs ++ postActionLogs
 
}
}