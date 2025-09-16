package com.adp.datacloud.ds

import com.adp.datacloud.writers.ParquetHiveDataFrameWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{ array, explode, lit, struct }
import scala.collection.immutable.ListMap
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import scala.xml.XML
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.StringIndexer
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext
import com.adp.datacloud.writers.DelimitedHiveDataFrameWriter
import com.adp.datacloud.writers.HiveDataFrameWriter
import com.adp.datacloud.cli.RecommenderConfig
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.SparkFiles



object RecommenderEngine {
  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    val recommenderConfig = com.adp.datacloud.cli.RecommenderOptions.parse(args)
    implicit val sparkSession = SparkSession
      .builder()
      .appName("Recommender Engine")
      .config("hive.exec.max.dynamic.partitions.pernode", "2000")
      .config("hive.exec.max.dynamic.partitions", "20000")
      .enableHiveSupport()
      .getOrCreate()

     val filesString = recommenderConfig.files
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

    computeRecommendations(recommenderConfig)

   }

   private def computeRecommendations(recommenderConfig: RecommenderConfig)(implicit
      sparkSession: SparkSession) = {

    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    def generateDF(sqlvar: String) = if (recommenderConfig.localParquetInputMode) {
      sparkSession.read.format("parquet").load(recommenderConfig.input)
    } else {
      val df = recommenderConfig.distributionSpec match {
        case Some(x) => sparkSession.sql(sqlvar).repartition(recommenderConfig.sparkShufflePartitions, x.map { col(_) }: _*)
        case None    => sparkSession.sql(sqlvar).repartition(recommenderConfig.sparkShufflePartitions)
      }

      if (recommenderConfig.enableCheckpoint) {
        val persistedDF = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
        println(persistedDF.count) // call an action to ensure the frame is persisted
        persistedDF
        /*persistedDF.rdd.checkpoint
        val checkpointedDF = (recommenderConfig.distributionSpec match {
          case Some(x) => sparkSession.createDataFrame(persistedDF.rdd, persistedDF.schema).repartition(recommenderConfig.sparkShufflePartitions, x.map { col(_) }: _*)
          case None    => sparkSession.createDataFrame(persistedDF.rdd, persistedDF.schema).repartition(recommenderConfig.sparkShufflePartitions)
        }).persist(StorageLevel.MEMORY_AND_DISK_SER)
        persistedDF.unpersist(true)
        println(checkpointedDF.count)
        checkpointedDF*/
      } else df
    }

    def distributeUniformly = (dataFrame: DataFrame) => {
    recommenderConfig.distributionSpec match {
      case Some(x) => dataFrame.repartition(recommenderConfig.sparkShufflePartitions, x map { col(_) } :_*).persist(StorageLevel.MEMORY_AND_DISK_SER)
       case None => dataFrame.repartition(recommenderConfig.sparkShufflePartitions).persist(StorageLevel.MEMORY_AND_DISK_SER)
     }    
    } 

    def checkpointDataFrame = (dataFrame: DataFrame, sparkSession: SparkSession) => {
      val persistedDF = dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val checkpointedDF = distributeUniformly(persistedDF.checkpoint())
      /*persistedDF.rdd.checkpoint
      val checkpointedDF = sparkSession.createDataFrame(persistedDF.rdd, persistedDF.schema)
        .repartition(recommenderConfig.sparkShufflePartitions).persist(StorageLevel.MEMORY_AND_DISK_SER)*/
      persistedDF.unpersist(true)
      print("Record count after checkpoint = " + checkpointedDF.count) // Action to materialize
      checkpointedDF
    }
    sc.setCheckpointDir(recommenderConfig.checkpointDir)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.deleteOnExit(new org.apache.hadoop.fs.Path(sc.getCheckpointDir.get)) // Cleanup checkpoint directory on exit

    //val rootElement = XML.load(recommenderConfig.xmlFilePath)

    val xmlPath = if (new File(recommenderConfig.xmlFilePath).exists()) {
      // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
      recommenderConfig.xmlFilePath
    } else {
      SparkFiles.get(recommenderConfig.xmlFilePath)
    }

    val rootElement = XML.load(xmlPath)

    val recGroupName = (rootElement \ "name").text
    val dimensions = ((rootElement \\ "dimensions" \\ "dimension").map { x => x.text }).toList
    val timeDim = (rootElement \ "dimensions" \ "hierarchy").filter({ x => x.attribute("type").isDefined && x.attribute("type").get(0).text == "time" }).map { x => (x \ "dimension").map { _.text } toList }.flatten
    //timeDimension should be first in the list of dimensions.
    val dimCol = (timeDim ++ dimensions.diff(timeDim))
    val metaColumns = ((rootElement \\ "metacolumns" \\ "meta").map { x => x.text }).toList
    //val oneHotColumns = ((rootElement \\ "onehotColumns" \\ "onehot").map { x => x.text }).toList
    val oneHotColumns = ((rootElement \\ "onehotColumns" \\ "onehot").map { x => (x.text ,x.attribute("values") )})
    val lblencodeColumns = ((rootElement \\ "lblEncodeColumns" \\ "lblencode").map { x => x.text }).toList
    val baseDf = generateDF(recommenderConfig.sql).withColumn("dimension_comb", lit("null")).withColumn("hash_dimension_comb", lit("null")).withColumn("value", lit(1))

    //As it is hard to update the same column,alternatively creates a column for every dimension and filling the data as column name
    val dimMetaDf = (dimCol ++ metaColumns).foldLeft(baseDf) { (df, x) =>
      df.withColumn("dim_" + x, when(col(x).isNotNull && dimCol.contains(x), lit(x)).when(col(x).isNotNull && metaColumns.contains(x), col(x)).otherwise($"dimension_comb"))
    }

    //Updating the dimension_comb column with all not null dimensions.
    val dimConcatDf = dimMetaDf.withColumn("dimension_comb", regexp_replace(concat_ws("-", dimCol.map(x => col("dim_" + x)): _*), "(-?)null", "")).
      withColumn("hash_dimension_comb", regexp_replace(concat_ws("-", (dimCol ++ metaColumns).map(x => col("dim_" + x)): _*), "(-?)null", "")).
      select(dimMetaDf.columns.filter(x => !(dimCol ++ metaColumns).map(x => "dim_" + x).contains(x)).map(x => col(x)): _*)

    //Sort the dimension combination.
    val sorted_df = dimConcatDf.withColumn("dimension_comb", concat_ws("_", sort_array(split(col("dimension_comb"), "-")))).withColumn("hash_dimension_comb", concat_ws("_", sort_array(split(col("hash_dimension_comb"), "-"))))
    // LabelEncoding
    val lbl_encode_df = lblencodeColumns.foldLeft(sorted_df) { (df, col) =>
      val indexer = new StringIndexer().setInputCol(col).setOutputCol(col + "_index").fit(df)
      indexer.transform(df)
    }
    val ckPointDf = checkpointDataFrame(lbl_encode_df, sparkSession)
    //oneHotEncoding
    val one_hot_df = oneHotColumns.foldLeft(sorted_df) {(df,col) =>
      val dimenlist = col._2 match {
        case Some(y) => y.text.split(",")
        case None => df.rdd.map(r => r.getAs[Any](col._1)).distinct().collect().filter(x=>x!=null)
      }
      val groupDf = df.groupBy("ins_hash_val").pivot(col._1, dimenlist).agg(first("value"))
      val groupRenamedDf = dimenlist.foldLeft(groupDf.na.fill(0,groupDf.columns.toList)){(df,dimen) =>
      df.withColumnRenamed(dimen.toString(), col._1+"_"+dimen)
      }
      df.join(groupRenamedDf,Seq("ins_hash_val"))
    }
    //oneHotEncoding
    /*val one_hot_df = oneHotColumns.foldLeft(ckPointDf) { (df, col) =>
      val dimenlist = df.map(r => r.getAs[Any](col)).distinct().collect().filter(x => x != null)
      val groupDf = df.groupBy("ins_hash_val").pivot(col, dimenlist).agg(first("value"))
      val groupRenamedDf = dimenlist.foldLeft(groupDf.na.fill(0,groupDf.columns.toList)){(df,dimen) =>
         df.withColumnRenamed(dimen.toString(), col+"_"+dimen)
       }
       df.join(groupRenamedDf,Seq("ins_hash_val"))
    }*/

    val hiveWriter = HiveDataFrameWriter(recommenderConfig.saveFormat, recommenderConfig.outputPartitionSpec)
    hiveWriter.insertOverwrite(recommenderConfig.outputDB + "." + recGroupName + "_input", ckPointDf)
    hiveWriter.insertOverwrite(recommenderConfig.outputDB + "." + recGroupName, one_hot_df)

  }
}