package com.adp.datacloud.ds

import com.adp.datacloud.cli.InsightConfig
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import java.io.File

/**
 * Spark Driver Program to generate insights
 */
object InsightDriver {

  private val logger = Logger.getLogger(getClass())
  private val RIGHT_SUFFIX = "_r"

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")
    val insightConfig = com.adp.datacloud.cli.InsightOptions.parse(args)

    implicit val sparkSession = SparkSession
      .builder()
      .appName(insightConfig.applicationName)
      .config("hive.exec.max.dynamic.partitions.pernode", "2000")
      .config("hive.exec.max.dynamic.partitions", "20000")
      .enableHiveSupport()
      .getOrCreate()

    val filesString = insightConfig.files
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

    computeInsights(insightConfig)

  }

  protected def computeInsights(insightConfig: InsightConfig)(implicit
                                                              sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    sc.setCheckpointDir(insightConfig.checkpointDir)

    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.deleteOnExit(
      new Path(sc.getCheckpointDir.get)
    ) // Cleanup checkpoint directory on exit

    def generateDF(sqlvar: String) =
      if (insightConfig.localParquetInputMode) {
        sparkSession.read.parquet(insightConfig.input)
      } else {
        val df = insightConfig.distributionSpec match {
          case Some(x) =>
            sparkSession
              .sql(sqlvar)
              .repartition(
                insightConfig.sparkShufflePartitions,
                x.map {
                  col(_)
                }: _*)
          case None =>
            sparkSession.sql(sqlvar).repartition(insightConfig.sparkShufflePartitions)
        }

        if (insightConfig.enableCheckpoint) {
          // This switch is poorly named. It just does a persist
          val persistedDF = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
          println(persistedDF.count) // call an action to ensure the frame is persisted
          persistedDF
        } else df

      }

    val thisDataFrame = generateDF(insightConfig.sql)
    val thatDataFrame = generateDF(insightConfig.compareSql)

    val insightXMLPath = if (new File(insightConfig.xmlFilePath).exists()) {
      // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
      insightConfig.xmlFilePath
    } else {
      SparkFiles.get(insightConfig.xmlFilePath)
    }

    val insightEngine = new InsightEngine(
      insightXMLPath,
      insightConfig.distributionSpec,
      insightConfig.sparkShufflePartitions)
    val insightsResult =
      insightEngine.build(thisDataFrame)(thatDataFrame.toDF(thatDataFrame.columns.map({
        _ + RIGHT_SUFFIX
      }): _*))

    val hiveWriter =
      HiveDataFrameWriter(insightConfig.saveFormat, insightConfig.outputPartitionSpec)
    hiveWriter.insertOverwrite(
      insightConfig.outputDB + "." + insightEngine.insightsGroupName,
      insightsResult)
  }
}
