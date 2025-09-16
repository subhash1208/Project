package com.adp.datacloud.ds

import com.adp.datacloud.cli.InsightConfig
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.xml.XML

/**
 * Spark Driver Program to generate Correlations
 */
object CubeCorrelation {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val cubeConfig = com.adp.datacloud.cli.InsightOptions.parse(args)

    implicit val sparkSession = SparkSession
      .builder()
      .appName("Cube Correlation")
      .enableHiveSupport()
      .config("hive.exec.max.dynamic.partitions.pernode", "2000")
      .config("hive.exec.max.dynamic.partitions", "20000")
      .getOrCreate()

    val filesString = cubeConfig.files
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

    computeCubeCorrelation(cubeConfig)

  }

  def computeCubeCorrelation(cubeConfig: InsightConfig)(implicit
      sparkSession: SparkSession) = {
    val sc = sparkSession.sparkContext

    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }
    sc.setCheckpointDir(cubeConfig.checkpointDir)
    val rootElement   = XML.load(cubeConfig.xmlFilePath)
    val corrGroupName = (rootElement \ "name").text
    val dimensions =
      ((rootElement \\ "dimensions" \\ "dimension").map { x => x.text }).toList
    val facts = ((rootElement \\ "facts" \\ "fact" \\ "name").map { x => x.text }).toList
    val analysisGroups = (rootElement \\ "dimension")
      .filter({ x =>
        x.attribute("partition_column")
          .isDefined && x.attribute("partition_column").get(0).text == "true"
      })
      .map { x => x.text } toList
    val selectColumns = dimensions ++ facts

    def generateDF(sqlvar: String) =
      if (cubeConfig.localParquetInputMode) {
        sparkSession.read.parquet(cubeConfig.input)
      } else {
        val df = cubeConfig.distributionSpec match {
          case Some(x) =>
            sparkSession
              .sql(sqlvar)
              .repartition(
                cubeConfig.sparkShufflePartitions,
                x.map {
                  col(_)
                }: _*)
          case None =>
            sparkSession.sql(sqlvar).repartition(cubeConfig.sparkShufflePartitions)
        }

        if (cubeConfig.enableCheckpoint) {
          val persistedDF = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
          persistedDF.rdd.checkpoint
          val checkpointedDF = (cubeConfig.distributionSpec match {
            case Some(x) =>
              sparkSession
                .createDataFrame(persistedDF.rdd, persistedDF.schema)
                .repartition(
                  cubeConfig.sparkShufflePartitions,
                  x.map {
                    col(_)
                  }: _*)
            case None =>
              sparkSession
                .createDataFrame(persistedDF.rdd, persistedDF.schema)
                .repartition(cubeConfig.sparkShufflePartitions)
          }).persist(StorageLevel.MEMORY_AND_DISK_SER)
          persistedDF.unpersist(true)
          println(checkpointedDF.count)
          checkpointedDF
        } else df
      }

    val allDimensions = (rootElement \\ "dimension").map { x =>
      (
        x.text,
        x.attribute("alwayson") match {
          case Some(y) => y.text.toBoolean
          case None    => false
        },
        x.attribute("type") match {
          case Some(y) => y.text
          case None    => "Normal"
        },
        x.attribute("differences") match {
          case Some(y) => y.text.split(",").map { z => z.toInt } toList
          case None    => List[Int]()
        })
    }

    val cubeDimensions  = allDimensions.filter(x => !x._2)
    val fixedDimensions = allDimensions.filter(x => x._2).map(x => x._1)

    val cubeDf =
      generateDF(cubeConfig.sql).select(selectColumns.head, selectColumns.tail: _*)
    val partitions = ((rootElement \\ "dimension")
      filter { z =>
        z.attribute("window_partition")
          .isDefined && z.attribute("window_partition").get(0).text == "true"
      }
      map ({
        _.text
      }) toList)
    val orderList = ((rootElement \\ "dimension")
      filter { z =>
        z.attribute("window_order")
          .isDefined && z.attribute("window_order").get(0).text == "true"
      }
      map ({
        _.text
      }) toList)

    val diffVal = allDimensions.filter(x => x._3 == "ephemeral").map(x => (x._4))

    val windowFacts = (facts).map(x => (x, diffVal(0)))
    val partitionSpec = partitions.map({
      col(_)
    })

    val corrinputDf = windowFacts
      .foldRight(cubeDf)({ (fact, df) =>
        fact._2.foldRight(df)({ (period, dataFrame) =>
          val rankPartitionSpec =
            if (partitions.isEmpty) Window.orderBy(fact._1)
            else Window.partitionBy(partitionSpec: _*)
          dataFrame.withColumn(
            "lead_" + period + "_" + fact._1,
            lead(fact._1, period, 0).over(
              rankPartitionSpec.orderBy(orderList.mkString(","))))
        })
      })
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val corrFacts = facts ++ corrinputDf.columns.diff(selectColumns)
    val corrlist = (facts cross corrFacts)
      .filter(x => x._1 != x._2)
      .filter(x => !(x._2.contains(x._1)))
      .map(
        x =>
          (
            corr(x._1, x._2).alias("corr_" + x._1 + "_" + x._2),
            sum(when(col(x._1).isNotNull.and(col(x._2).isNotNull), lit(1)))
              .alias("cnt_" + x._1 + "_" + x._2)))
      .toSeq
    val listOfCondition = cubeDimensions.map({ y =>
      (
        allDimensions.foldRight(lit(true)) { (z, colSpec) =>
          colSpec.and(col(z._1).isNotNull)
        },
        y._1)
    })
    val newcorr = corrlist.flatMap { case (a, b) => List(a, b) } ++ List(
      count(lit(1)).alias("num_records"))
    val unionDf = listOfCondition
      .map { x =>
        corrinputDf
          .filter(x._1)
          .groupBy(fixedDimensions.head, fixedDimensions.tail: _*)
          .agg(newcorr.head, newcorr.tail: _*)
          .withColumn("dimension", lit(x._2))
      }
      .reduce((df1, df2) => df1.union(df2))

    val factsCross = (facts cross facts).filter(x => x._1 != x._2)
    val renamedDfLead = factsCross.foldLeft(unionDf)({ (dataFrame, column) =>
      dataFrame
        .withColumnRenamed(
          "corr_" + column._1 + "_" + column._2,
          "corr_" + column._1 + "_lead_0_" + column._2)
        .withColumnRenamed(
          "cnt_" + column._1 + "_" + column._2,
          "cnt_" + column._1 + "_lead_0_" + column._2)
    })
    val corrDf = factsCross.foldLeft(renamedDfLead)({ (dataFrame, column) =>
      dataFrame
        .withColumn(
          column._1 + "_" + column._2 + "_corr",
          //array(round(col("corr_"+column._1+"_"+column._2),2),round(col("corr_"+column._1+"_lead_1_"+column._2),2),round(col("corr_"+column._1+"_lead_2_"+column._2),2))
          array((Seq(0) ++ diffVal.flatten).map(y =>
            round(col("corr_" + column._1 + "_lead_" + y + "_" + column._2), 2)): _*))
        .withColumn(
          column._1 + "_" + column._2 + "_cnt",
          array((Seq(0) ++ diffVal.flatten).map(y =>
            col("cnt_" + column._1 + "_lead_" + y + "_" + column._2)): _*))
    })

    val finalDf = corrDf.select(
      corrDf.columns
        .filterNot(x => x.startsWith("corr_"))
        .filterNot(x => x.startsWith("cnt_"))
        .map(x => corrDf(x)): _*)

    val hiveWriter =
      HiveDataFrameWriter(cubeConfig.saveFormat, cubeConfig.outputPartitionSpec)
    hiveWriter.insertOverwrite(cubeConfig.outputDB + "." + corrGroupName, finalDf)
  }
}
