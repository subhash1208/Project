package com.adp.datacloud.ds.util

import com.adp.datacloud.cli.SnowDataProcessConfig
import org.apache.spark.sql.{ DataFrame, SparkSession } 
import org.apache.spark.sql.execution.datasources.jdbc.{ JdbcOptionsInWrite, JdbcUtils }

case class StepLogs (
    APPLICATION_ID : String,
    ACTION_TYPE : String,
    ACTION: String,
    TIME_DURATION: Long = 0L,
    STATUS : String = "PASS",
    STACK_TRACE : String = null)

object SnowProcessLogger{
  
  def apply(logs: List[StepLogs], 
            config: SnowDataProcessConfig)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    // Write Logs to DXR table
    val df = logs.toDF
    writeJobLogs(df, config)
  }
  
  def writeJobLogs(df: DataFrame,
                   config: SnowDataProcessConfig) = {
      val logTableOptions = new JdbcOptionsInWrite(config.dxrJdbcUrl, "SNOWFLAKE_PROCESS_LOGS", config.jdbcProperties)
      val conn = JdbcUtils.createConnectionFactory(logTableOptions)()    
      try {
        if (! JdbcUtils.tableExists(conn, logTableOptions))
          {
          val sourceFile = scala.io
                              .Source
                              .fromInputStream(getClass.getResourceAsStream(s"/create_snowprocess_logs.sql"))
          
          val createTableSql = try sourceFile.mkString.replace("&TABLE_NAME", "SNOWFLAKE_PROCESS_LOGS")
          finally sourceFile.close
          val statement = conn.createStatement
          createTableSql.split(";").foreach(statement.addBatch(_))
          try {
            statement.executeBatch
              }   
          finally {
          statement.close
        }
      }
      JdbcUtils.saveTable(df, None, true, logTableOptions)
      
    } catch {
      case e: Exception => {
        //If log write fails then print the data and raise exception to exit.
        df.show(1000,false)
        println("Error while writing Snowflake job logs : ", e)
        e.printStackTrace
        throw new Exception
      }
    }
    finally {
      conn.close }

  }
}