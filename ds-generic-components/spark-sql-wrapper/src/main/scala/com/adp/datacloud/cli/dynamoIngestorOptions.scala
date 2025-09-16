package com.adp.datacloud.cli

import scala.io.Source

import org.apache.spark.SparkFiles

case class DynamoIngestorConfig(
    file:                   String = "",
    inputParams:            List[String]   = List(),
    dynamoParams:           List[String]   = List(),
    dynamodbTableName:      String = "",
    partitionKey:           String = "id",
    sortKey:                Option[String] = None,
    isProvisioned:          Boolean = false,
    writeThroughput:        Int = 1000,
    readThroughput:         Int = 1000,
    writeParallelism:       Int = 75,
    files: Option[String]                = None
) {
  def sql = {
    val sourcequery = (if (new java.io.File(file).exists()) {
          // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
          Source.fromFile(file)("UTF-8")
        } else {
          Source.fromFile(SparkFiles.get(file))("UTF-8")
        }).getLines().map { _.replaceAll("^\\s*\\-\\-.*$", "").replaceAll("^\\s*$", "") }.filter(!_.isEmpty()).mkString("\n")

    //Replacing the hiveconf variables
    inputParams.foldLeft(sourcequery) { (y, x) =>
      ("\\$\\{hiveconf:" + x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))
    }.replaceAll("\\$\\{hiveconf:[a-zA-Z0-9]+\\}", "")
  }
  
  def getParam(paramName: String) = {
    val parameterMap = inputParams.map({x => x.split("=")(0) -> x.split("=")(1)}).toMap
    parameterMap.get(paramName)
  }
  
  lazy val dynamoConfiguration = {
    dynamoParams.map({x => x.split("=")(0) -> x.split("=")(1)}).toMap
  }
  
}

object dynamoIngestorOptions {
  
  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[DynamoIngestorConfig]("DynamoDB Data Ingestor") {
      
      head("DynamoDB Data Ingestor", this.getClass.getPackage().getImplementationVersion())
      help("help").text("Prints usage")

      override def showUsageOnError = true

      opt[String]('f', "ingest-sql").required().action((x, c) =>
        c.copy(file = x)).text("Input Sql string")

      opt[String]('c', "hiveconf").unbounded().action((x, c) =>
        c.copy(inputParams = c.inputParams ++ List(x))).text("Hiveconf variables to replaced in query")
        
      opt[String]('c', "dynamoconf").unbounded().action((x, c) =>
        c.copy(inputParams = c.dynamoParams ++ List(x))).text("Dynamo client configuration")

      opt[String]('t', "dynamo-table-name").required().action((x, c) =>
        c.copy(dynamodbTableName = x)).text("DynamoDB Table to write to")
        
      opt[String]('p', "dynamo-partition-key").required().action((x, c) =>
        c.copy(partitionKey = x)).text("Partition Key on DynamoDB Table")
        
      opt[String]('s', "dynamo-sort-key").action((x, c) =>
        c.copy(sortKey = Some(x))).text("Sort Key on DynamoDB Table")
        
      opt[Boolean]("provisioned-mode").action((x, c) =>
        c.copy(isProvisioned = x)).text("Throughput mode. Default false")
        
      opt[Int]("read-throughput").action((x, c) =>
        c.copy(readThroughput = x)).text("Read Throughput. Default 1000")
        
      opt[Int]("write-throughput").action((x, c) =>
        c.copy(writeThroughput = x)).text("Write Throughput. Default 1000")
        
      opt[Int]("write-parallelism").action((x, c) =>
        c.copy(writeParallelism = x)).text("Write parallelism on target. Default 75")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")
        
    }
    
    // parser.parse returns Option[C]
    parser.parse(args, DynamoIngestorConfig()) match {
      case Some(config) => config
      case None => {
        System.exit(1)
        null // Error message would have been displayed
      }
    }

  }
  
}