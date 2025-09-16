package com.adp.datacloud.ds

import com.adp.datacloud.cli.HierarchyConfig
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Computes orgstructure and exploded tables for hierarchies
 *
 */
object orgStructure {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val layersConfig: HierarchyConfig = com.adp.datacloud.cli.hierarchyOptions.parse(args)

    implicit val sparkSession = SparkSession
      .builder()
      .appName(layersConfig.applicationName)
      .enableHiveSupport()
      .getOrCreate()

    val filesString = layersConfig.files
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

    buildHierarchy(layersConfig)
  }

  def buildHierarchy(layersConfig: HierarchyConfig)(implicit
      sparkSession: SparkSession): Unit = {

    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict");
    sparkSession.sparkContext.setCheckpointDir("/tmp/checkpoints/")

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val hdfs       = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.deleteOnExit(
      new org.apache.hadoop.fs.Path(sparkSession.sparkContext.getCheckpointDir.get)
    ) // Cleanup checkpoint directory on exit

    val fkColumn            = layersConfig.fkColumnName
    val pkColumn            = layersConfig.pkColumnName
    val groupColumns        = layersConfig.groupColumns
    val startWithExpression = layersConfig.startWithRowsExpression

    val dataFrame = {
      val df = layersConfig.distributionSpec match {
        case Some(x) => {
          // Same distribution spec as can be noticed in CubeBuilder
          val initialDf = sparkSession.sql(layersConfig.sql)
          val maxRecordsPerTask = initialDf
            .groupBy(x.split(",").map { col(_) }: _*)
            .count()
            .select(avg("count"))
            .head
            .get(0)
            .asInstanceOf[Double]

          val spec = Window
            .partitionBy(x.split(",").map { col(_) }: _*)
            .orderBy(col(x.split(",").head))
          initialDf
            .withColumn(
              "bucket_number",
              ceil(row_number.over(spec) / lit(maxRecordsPerTask)))
            .repartition((x.split(",") ++ List("bucket_number")).map {
              col(_)
            }: _*)
        }
        case None => sparkSession.sql(layersConfig.sql)
      }

      if (layersConfig.enableCheckpoint) {
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)
      } else df
    }.withColumn("layer", lit(0))
      .withColumn("is_root", expr(startWithExpression))

    val maxLayers = 15

    val baseDf = dataFrame
      .select(fkColumn, groupColumns ++ List(pkColumn, "is_root"): _*)
      .withColumn(
        "layer",
        when(col("is_root").equalTo(lit(true)), lit(1)).otherwise(lit(0)))
      .withColumn(
        "path_accumulator",
        when(col("is_root").equalTo(lit(true)), array(lit("ONE")).cast("array<string>"))
          .otherwise(array().cast("array<string>")))

    // Loop to create layers
    val resultDf = (1 to maxLayers).foldLeft(
      (baseDf, Some(baseDf.filter("layer = 1")): Option[DataFrame], 0)) { (y, x) =>
      {

        y._2 match {

          case Some(prevLayerRecordsDf) => {

            //            val myfunc = (x: String, xs: Seq[String]) => {xs :+ x}

            val appendToArray = udf((x: String, xs: Seq[String]) => { xs :+ x })

            val parentsDf = prevLayerRecordsDf
              .select(pkColumn, List("path_accumulator") ++ groupColumns: _*)
              .withColumnRenamed(pkColumn, fkColumn)
              .withColumn(
                "path_accumulator",
                appendToArray(col(fkColumn), col("path_accumulator")))

            // While computing the current layer employees its important to filter with layer=0 to prevent accidental cycles in hierarchy
            val currentLayerFiltered = y._1.drop("path_accumulator").filter("layer = 0")
            val currentLayerRecordsDf = currentLayerFiltered
              .join(parentsDf, groupColumns ++ List(fkColumn))
              .select((groupColumns ++ List(pkColumn, fkColumn)).map { x =>
                currentLayerFiltered(x)
              } ++ List(col("path_accumulator")): _*)
              .withColumn("layer_current", lit(x + 1))
              .drop(fkColumn)

            val updatedBaseDf = y._1
              .join(
                currentLayerRecordsDf.withColumnRenamed(
                  "path_accumulator",
                  "parents_path"),
                groupColumns ++ List(pkColumn),
                "left")
              .withColumn(
                "path_accumulator",
                when(isnull(col("parents_path")), col("path_accumulator")).otherwise(
                  col("parents_path")))
              .drop("parents_path")
              .withColumn(
                "layer",
                when(col("layer_current").isNull, col("layer"))
                  .otherwise(col("layer_current")))
              .drop("layer_current")

            updatedBaseDf.persist()
            currentLayerRecordsDf.persist()
            updatedBaseDf.rdd.checkpoint() // truncate lineage
            currentLayerRecordsDf.rdd.checkpoint() // truncate lineage

            val count = updatedBaseDf.count() // Force the checkpoint

            val managersForNextIteration =
              if (currentLayerRecordsDf.count() == 0)
                None
              else
                Some(
                  sparkSession.createDataFrame(
                    currentLayerRecordsDf.rdd,
                    currentLayerRecordsDf.schema))
            val updatedBaseDfNew =
              sparkSession.createDataFrame(updatedBaseDf.rdd, updatedBaseDf.schema)

            y._1.unpersist() // Spark automatically does this, but still being thorough about it
            prevLayerRecordsDf
              .unpersist() // Spark automatically does this, but still being thorough about it
            // Note : It is important to unpersist "after" the "count" action. Otherwise the caching benefits are not realized

            (updatedBaseDfNew, managersForNextIteration, x)

          }

          case None => {
            (y._1, None, y._3)
          } // This case is used for early termination (and thus not doing all the possible 30 joins for layer depth)
        }

      }
    }

    val layers_range = (1 to (resultDf._3 - 1))

    val hierarchyDf = layers_range.foldLeft(resultDf._1) { (df, x) =>
      df.withColumn("layer_" + x + "_parent", col("path_accumulator")(x))
    }

    hierarchyDf.persist()

    val dfDirReportsAdded = hierarchyDf
      .join(
        baseDf
          .groupBy(fkColumn, groupColumns: _*)
          .agg(count(lit(1)) as "direct_children")
          .withColumnRenamed(fkColumn, pkColumn),
        groupColumns ++ Seq(pkColumn),
        "left")
      .na
      .fill(0)

    val emptyDf = dfDirReportsAdded
      .select(pkColumn, groupColumns: _*)
      .filter("1 = 2")
      .withColumn("total_children", lit(0))

    val dfTotalChildrenAdded = layers_range.foldLeft(emptyDf) { (df, x) =>
      {
        val k = dfDirReportsAdded
          .filter("layer_" + x + "_parent IS NOT NULL")
          .groupBy("layer_" + x + "_parent", groupColumns: _*)
          .agg(count(lit(1)).alias("total_children"))
          .withColumnRenamed("layer_" + x + "_parent", pkColumn)
          .withColumnRenamed("count", "total_children")
          .select(pkColumn, groupColumns :+ "total_children": _*)
        df.union(k)
      }
    }

    val hierarchyResult = dfDirReportsAdded
      .join(dfTotalChildrenAdded, groupColumns ++ Seq(pkColumn), "left")
      .withColumn(
        "indirect_children",
        (col("total_children") - col("direct_children")).cast("long"))
      .na
      .fill(0, List("direct_children", "total_children", "indirect_children"))

    val explodedDf = layers_range
      .foldLeft(
        baseDf
          .select(groupColumns :+ pkColumn :+ fkColumn :+ "layer" map { col(_) }: _*)
          .withColumn("level_from_parent", lit(null.asInstanceOf[Int]))
          .withColumn("mngr_layer", lit(null.asInstanceOf[Int]))
          .filter("1 = 2"))({ (df, x) =>
        {
          val returnDf = hierarchyDf
            .select("layer_" + x + "_parent", groupColumns :+ pkColumn :+ "layer": _*)
            .withColumn("mngr_layer", lit(x))
            .withColumnRenamed("layer_" + x + "_parent", fkColumn)
            .withColumn("level_from_parent", col("layer").minus(lit(x)))
            .select(
              groupColumns :+ pkColumn :+ fkColumn :+ "layer" :+ "level_from_parent" :+ "mngr_layer" map {
                col(_)
              }: _*)
          returnDf.union(df)
        }
      })
      .filter("layer is not null and (mngr_pers_obj_id is not null or layer = 1) and level_from_parent >= 0")

    val layersResult = (resultDf._3 to maxLayers).foldLeft(
      hierarchyResult.select(
        groupColumns
          ++ List(
            pkColumn,
            fkColumn,
            "layer",
            "direct_children",
            "indirect_children",
            "total_children",
            "path_accumulator")
          ++ (layers_range.map { x => "layer_" + x + "_parent" }) map { col(_) }: _*))(
      (df, x) => {
        df.withColumn("layer_" + x + "_parent", lit(null).cast("string"))
      })

    val addonColumns =
      dataFrame.columns.filter({ x => x != "is_root" }).diff(layersResult.columns)

    val finalResult = if (addonColumns.isEmpty) {
      layersResult
    } else {
      layersResult
        .join(dataFrame, groupColumns ++ List(pkColumn))
        .select((layersResult.columns map { layersResult(_) }) ++ (addonColumns map {
          col(_)
        }): _*)
    }

    val hiveWriter =
      HiveDataFrameWriter(layersConfig.saveFormat, layersConfig.outputPartitionSpec)
    hiveWriter.insertOverwrite(
      layersConfig.outputDBName + "." + layersConfig.outputHierarchyTable,
      finalResult)
    hiveWriter.insertOverwrite(
      layersConfig.outputDBName + "." + layersConfig.outputExplodedHierarchyTable,
      explodedDf)

  }

}
