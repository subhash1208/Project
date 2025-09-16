package com.adp.datacloud.ds

import com.adp.datacloud.cli.CubeConfig
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileApprox
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

/**
 * Spark Driver Program to begin cube build
 */
object CubeBuilder {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val cubeConfig = com.adp.datacloud.cli.CubeOptions.parse(args)

    implicit val sparkSession: SparkSession = SparkSession.builder
      .appName(cubeConfig.applicationName)
      .enableHiveSupport()
      .getOrCreate()

    if (cubeConfig.filesList.nonEmpty) {
      cubeConfig.filesList
        .foreach(x => {
          logger.info(s"DEPENDENCY_FILE: adding $x to sparkFiles")
          sparkSession.sparkContext.addFile(x)
        })
    }
    computeCube(cubeConfig)
  }

  def setLimitedGroupingSetsProperties(
      xmlConfigString: String,
      minCubeDepth: Int,
      maxCubeDepth: Int): Unit = {
    // Temporarily build config to get the number of mandatory parameters for building a Rule
    val tempCubeProcessor = new CubeProcessor(xmlConfigString)
    logger.info(s"TEMP_CUBE_PROCESSOR_PROPS: ${tempCubeProcessor.toJsonString()}")

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    System.setProperty("cube.base.dimensions", write(tempCubeProcessor.baseDimensions))
    System.setProperty("cube.min.cube.depth", minCubeDepth.toString)
    System.setProperty("cube.max.cube.depth", maxCubeDepth.toString)
    System.setProperty("cube.grouping.sets", write(tempCubeProcessor.groupingSets))
    System.setProperty(
      "cube.mandatory.dimensions",
      write(tempCubeProcessor.mandatoryDimensions))
    System.setProperty("cube.hierarchies", write(tempCubeProcessor.hierarchies))
    System.setProperty("cube.xor.blacklists", write(tempCubeProcessor.xorBlacklists))
    System.setProperty(
      "cube.limited.depth.dimensions",
      write(tempCubeProcessor.limitedDepthDimensions))
  }

  def computeCube(cubeConfig: CubeConfig)(implicit sparkSession: SparkSession): Any = {

    val sparkSqlExtensions = sparkSession.conf.get("spark.sql.extensions", "")
    
    require(
      sparkSqlExtensions.contains("com.adp.datacloud.ds.LimitedGroupingSetsExtension"),
      "com.adp.datacloud.ds.LimitedGroupingSetsExtension is missing please check if the spark.sql.extensions is properly set")

    setLimitedGroupingSetsProperties(
      cubeConfig.xmlConfigString,
      cubeConfig.minCubeDepth,
      cubeConfig.maxCubeDepth)

    // Add the older hive_percentile_approx function with another name.
    // This version seems to be more efficient than Sparks new approxQuantile
    sparkSession.sqlContext.sql(
      s"CREATE OR REPLACE TEMPORARY FUNCTION hive_percentile_approx AS '" +
        s"${classOf[GenericUDAFPercentileApprox].getName}'")

    val sc = sparkSession.sparkContext

    val isLocalParquetInputMode = cubeConfig.localParquetInputMode

    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict");
    val shufflePartitions =
      sparkSession.conf.get("spark.sql.shuffle.partitions", "200").toInt

    // TODO: use a s3 location for checkpoint
    sc.setCheckpointDir(cubeConfig.checkpointDir)

    val hadoopConf = sc.hadoopConfiguration
    val hdfs       = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.deleteOnExit(
      new org.apache.hadoop.fs.Path(sc.getCheckpointDir.get)
    ) // Cleanup checkpoint directory on exit

    val inputFrame =
      if (isLocalParquetInputMode)
        sparkSession.read.parquet(cubeConfig.input)
      else {
        logger.info("Executing -- " + cubeConfig.sql)
        sparkSession.sql(cubeConfig.sql)
      }

    // Logic to initialize TimeCube
    val tmpFolder = new Path(s"/tmp/${sc.sparkUser}/${sc.applicationId}")
    hdfs.mkdirs(tmpFolder)
    val inputStream = Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream("t_dim_day_rollup/t_dim_day_rollup.snappy.parquet")
    val hdfsWritePath = new Path(
      s"/tmp/${sc.sparkUser}/${sc.applicationId}/t_dim_day_rollup.snappy.parquet"
    ) // This points to a HDFS path
    val outputStream = hdfs.create(hdfsWritePath);
    outputStream.write(IOUtils.toByteArray(inputStream))
    outputStream.close()
    // Create the timeRollup dataframe and immediately checkpoint it.
    val timeRollup = sparkSession.read.parquet(
      s"/tmp/${sc.sparkUser}/${sc.applicationId}/t_dim_day_rollup.snappy.parquet")
    hdfs.deleteOnExit(tmpFolder) // Recursively delete the temporary folder
    // We are not doing any exception handling here intentionally and letting the calling program fail if timeCube could not be initialized

    val cubeProcessor = new CubeProcessor(
      cubeConfig.xmlConfigString,
      Some(timeRollup),
      cubeConfig.ignoreNullGroupings,
      cubeConfig.limitedTestMode)

    val repartitionedDataFrame = cubeConfig.distributionSpec match {
      case Some(x) =>
        inputFrame.repartition(
          shufflePartitions,
          x.split(",").map {
            col(_)
          }: _*)
      case None => {
        // If the cardinality of mandatory dimensions is close to shuffle-partitions, then it helps to repartition the input dataframe by these columns
        if (!cubeProcessor.mandatoryDimensions.isEmpty && inputFrame
            .groupBy(cubeProcessor.mandatoryDimensions map {
              col(_)
            }: _*)
            .count()
            .count() >= shufflePartitions * 0.7)
          inputFrame.repartition(
            shufflePartitions,
            cubeProcessor.mandatoryDimensions map {
              col(_)
            }: _*)
        else
          inputFrame.repartition(shufflePartitions)
      }
    }

    val skipCubeBuild = if (cubeConfig.scanDimensions) {
      // Scan dimensions and check for any possible warnings
      val listOfSummaries = cubeProcessor.scanDimensions(repartitionedDataFrame)
      val summariesToFlag = listOfSummaries.filter {
        !_.listOfWarnings.isEmpty
      }
      summariesToFlag foreach logger.warn
      summariesToFlag.isEmpty
    } else false

    if (!skipCubeBuild) {

      val df = cubeProcessor.build(repartitionedDataFrame)

      if (cubeConfig.onlyPlan) {
        logger.info("Not building cube. Just generating the plan...")
        df.explain(true)
      } else {
        val tableFullName = cubeConfig.outputDBName + "." + cubeConfig.cubeName
        val hiveWriter =
          HiveDataFrameWriter(cubeConfig.saveFormat, cubeConfig.outputPartitionSpec)
        hiveWriter.insertSafely(tableFullName, df, cubeConfig.overwriteDestination)
      }

    } else {
      logger.warn(
        "Skipping cube build because of warnings during dimension scan phase!!!")
    }

  }

}
