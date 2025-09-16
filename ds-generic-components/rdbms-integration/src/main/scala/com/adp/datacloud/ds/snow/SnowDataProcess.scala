package com.adp.datacloud.ds.snow

import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, SaveMode}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import scala.collection.Seq
import com.adp.datacloud.cli.SnowDataProcessConfig
import com.adp.datacloud.ds.util.SnowProcessLogger


object SnowDataProcess {
  
  private val logger = Logger.getLogger(getClass)
 
  def main(args : Array[String]) {
    
     println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")
     val snowDataProcessConfig = com.adp.datacloud.cli.SnowDataProcessorOptions.parse(args)
     println(SnowDataProcessConfig)

     implicit val sparkSession: SparkSession = SparkSession
          .builder
          .appName(snowDataProcessConfig.appName)
          .enableHiveSupport
          .getOrCreate

    val filesString = snowDataProcessConfig.files
      .getOrElse("")
      .trim

    if (filesString.nonEmpty) {
      filesString
        .split(",")
        .foreach(x => {
          logger.info(s"DEPENDENCY_FILE: adding $x to sparkFiles")
          sparkSession.sparkContext.addFile(x)
        })
    }

    tryProcess(snowDataProcessConfig)
       
  }
  
  // triggers the export or extract as per the processMode from config  
  def tryProcess(config : SnowDataProcessConfig)(implicit sparkSession: SparkSession) = {
    
         import sparkSession.implicits._
    
         val processor = config.processMode match {
             case "export" => { 
                          logger.info("Starting with processs mode as Export.")
                          val ingestStatus = new SnowIngest(config).ingest
                          logger.info("Export process completed.")
                          println("Export process completed.")    
                          ingestStatus  }
             case "extract" => {
                          logger.info("Starting with processs mode as Extract.")
                          new SnowExtract
                          logger.info("Extract process completed.")
                          println("Extract process completed.") 
                          List()  } 
       }
	   logger.info("Data processing completed.")
      println("Data processing completed.")

// Appending logs to the log table
      
      SnowProcessLogger(processor,config)
      logger.info("Logs saved successfully.")
      println("Logs saved successfully.")

  }
}