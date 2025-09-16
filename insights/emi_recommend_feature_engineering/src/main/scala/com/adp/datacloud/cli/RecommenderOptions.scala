package com.adp.datacloud.cli

import org.apache.spark.SparkFiles

/**
 * Cubing Framework Command Line Options utility class. 
 */
case class RecommenderConfig(
    xmlFilePath: String         = null,
    input: String               = null,
    compare: Option[String]     = None,
    outputDB: String            = null,
    outputPartitionSpec: Option[String] = None,
    inputPartitionColumn: Option[String] = None,
    inputPartitionColumnValues: Option[List[String]] = None,
    sparkShufflePartitions: Int = 200,
    enableTungsten: Boolean = true,
    enableCheckpoint: Boolean = true,
    checkpointDir: String = "/tmp/checkpoints/",
    distributionSpec: Option[List[String]] = None,
    applicationName: String = "ADP Insights recommendatins FE",
    localParquetInputMode: Boolean = false,
    saveFormat: String = "parquet",
    minimumPercentageDifference:Double = 5,
    inputPartitionSpec: List[String] = List(),
    inputParams : List[String] =       List(),
    comparePartitionSpec: List[String] = List(),
    files: Option[String]    = None) {

  def readSql(input: String) = {
    val sql = if (input.endsWith(".sql")) {

      val sourceFile = if(new java.io.File(input).exists()){
      
    //file resides in driver working directory and can be accessed directly 
     scala.io.Source.fromFile(input)
      }
      else {
        scala.io.Source.fromFile(SparkFiles.get(input))
      }
        
      "select * from (" + (try sourceFile.mkString finally sourceFile.close()) + "\n) ignored_alias"
    } else {
      "select * from " + input
    }
    
    //Replacing the hiveconf variables
      inputParams.foldLeft(sql){(y,x) =>    
        ("\\$\\{hiveconf:"+ x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))
      }
  }
  
  def applyPartitionSpec(sql: String, partitionSpec: List[String]) = {    
   if (!partitionSpec.isEmpty) {
     sql + " where " + partitionSpec.foldLeft("1=1"){(y,x) => 
       y + " and " + x.split("=")(0) + " in (" + x.split("=")(1).split(",").mkString("'", "', '","'") + ")"
      }
   } else sql
  }
  
  def sql = {
    applyPartitionSpec(readSql(input), inputPartitionSpec)
  }  
  
  def compareSql = {
    compare match {
      case Some(x) => applyPartitionSpec(readSql(x), comparePartitionSpec)
      case None => sql
    }
  } 
  
}

object RecommenderOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[RecommenderConfig]("Recommender Engine") {
      head("RecommenderEngine", this.getClass.getPackage().getImplementationVersion())

      override def showUsageOnError = true

      opt[String]('a', "application-name").action((x, c) =>
        c.copy(applicationName = x)).text("Name of the application.")

      opt[String]('x', "xml-config-file").required().action((x, c) =>
        c.copy(xmlFilePath = x)).text("XML config file relative path from current working directory. Mandatory")

      // TODO: Add validation for output-spec
      opt[String]('i', "input-spec").required().action((x, c) =>
        c.copy(input = x)).text("Input Either of 1) Fully qualified table or 2) Sql file")
      
      opt[String]('p', "input-partition-spec").unbounded().action((x, c) =>        
        c.copy(inputPartitionSpec = c.inputPartitionSpec ++ List(x))).text("Optional partition spec to filter the input sql with")  
      
      opt[String]('c', "comparsion-spec").action((x, c) =>
        c.copy(compare = Some(x))).text("Compare Either of 1) Fully qualified table or 2) Sql file")
        
      opt[String]('s', "compare-partition-spec").unbounded().action((x, c) =>        
        c.copy(comparePartitionSpec = c.comparePartitionSpec ++ List(x))).text("Optional partition spec to filter the compare sql with")  
        
      opt[Boolean]('l', "local-parquet-input-mode").action((x, c) =>
        c.copy(localParquetInputMode = x)).text("Treat the inputs and outputs as local/hdfs paths to parquet files. Defaults to false. If set to true, the input-partitions-* parameters are ignored")

      // TODO: Add validation for output-spec
      opt[String]('o', "output-db").required().action((x, c) =>
        c.copy(outputDB = x)).text("Hive DB where output tables should be saved. Ignored if hdfs-output-path is provided")
      
      opt[String]('t', "output-partition-spec").validate(x => if (x.split("=").size <= 2) success else failure("Incorrect output-partition-spec")).action((x, c) =>
        c.copy(outputPartitionSpec = Some(x))).text("Output Partition Spec (column=value). Optional. Example pp=2017q1")
      
      opt[Int]('n', "num-shuffle-partitions").action((x, c) =>
        c.copy(sparkShufflePartitions = x)).text("Number of partitions to be used for spark shuffle. Defaults to 200")

      opt[String]('d', "distribute-by").action((x, c) =>
        c.copy(distributionSpec = Some(x.split(",").toList))).text("Distribute input by the specified columns prior to aggregate processing.")

      opt[Boolean]('g', "enable-tungsten").action((x, c) =>
        c.copy(enableTungsten = x)).text("Enable/Disable tungsten (true/false). Defaults to true. Disable this while using Spark 1.5")

      opt[Boolean]('e', "enable-checkpoint").action((x, c) =>
        c.copy(enableCheckpoint = x)).text("Persist and Checkpoint the input dataframe prior to aggregate processing. Defaults to false.")

      opt[String]('r', "reliable-checkpoint-dir").action((x, c) =>
        c.copy(checkpointDir = x)).text("RDD Checkpoint directory. Defaults to /tmp/checkpoints/")
        
      opt[Double]("min-pctg-diff").action((x, c) =>
        c.copy(minimumPercentageDifference = x)).text("Minimum value of percentage difference for Diff based insights. Default is 5%")
      
      opt[String]("hiveconf").unbounded().action((x, c) =>        
        c.copy(inputParams = c.inputParams ++ List(x))).text("Hiveconf variables to replaced in input")  

      opt[String]('s', "save-format").action((x, c) =>
        c.copy(saveFormat = x)).text("Output Table format. Supported values are 'parquet' and 'text' ").validate(x =>
        if (List("parquet", "text").contains(x)) success
        else failure("save-format option must be one of 'parquet' and 'text'"))

        opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

      help("help").text("Prints usage")

      note("ADP Insight Recommendation Feature Engineering Framework. \n")

    }

    // parser.parse returns Option[C]
    parser.parse(args, RecommenderConfig()).get

  }

}