package com.adp.datacloud.ds.rosie

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{array, explode, lit, struct}
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
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object Recommender_Engine_Test {
  def main(args: Array[String]) {

    val conf                = new SparkConf(true).setAppName("Recommender Engine").setMaster("local")
    val sc                  = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val rootElement = XML.load("src/test/resources/xmls/rosie_recommendations.xml")
    val df          = sqlContext.read.parquet("src/test/resources/rosie_recommendations.parquet")

    val recGroupName = (rootElement \ "name").text
    val dimensions =
      ((rootElement \\ "dimensions" \\ "dimension").map { x => x.text }).toList
    val timeDim = (rootElement \ "dimensions" \ "hierarchy")
      .filter({ x =>
        x.attribute("type").isDefined && x.attribute("type").get(0).text == "time"
      })
      .map { x => (x \ "dimension").map { _.text } toList }
      .flatten
    val dimCol = (timeDim ++ dimensions.diff(timeDim))
    val metaColumns =
      ((rootElement \\ "metacolumns" \\ "meta").map { x => x.text }).toList
    val oneHotColumns =
      ((rootElement \\ "onehotColumns" \\ "onehot").map { x => x.text }).toList
    //val oneHotColumns = ((rootElement \\ "onehotColumns" \\ "onehot").map { x => (x.text ,x.attribute("values") )})
    val lblencodeColumns =
      ((rootElement \\ "lblEncodeColumns" \\ "lblencode").map { x => x.text }).toList
    val baseDf = sqlContext.read
      .parquet("src/test/resources/rosie_recommendations.parquet")
      .withColumn("dimension_comb", lit("null"))
      .withColumn("hash_dimension_comb", lit("null"))
      .withColumn("value", lit(1))
    dimCol.foreach(println)
    metaColumns.foreach(println)
    val dimMetaDf = (dimCol ++ metaColumns).foldLeft(baseDf) { (df, x) =>
      df.withColumn(
        "dim_" + x,
        when(col(x).isNotNull && dimCol.contains(x), lit(x))
          .when(col(x).isNotNull && metaColumns.contains(x), col(x))
          .otherwise($"dimension_comb"))
    }
    //dimMetaDf.show()
    //dimMetaDf.printSchema()

    val dimConcatDf = dimMetaDf
      .withColumn(
        "dimension_comb",
        regexp_replace(
          concat_ws("-", dimCol.map(x => col("dim_" + x)): _*),
          "(-?)null",
          ""))
      .withColumn(
        "hash_dimension_comb",
        regexp_replace(
          concat_ws("-", (dimCol ++ metaColumns).map(x => col("dim_" + x)): _*),
          "(-?)null",
          ""))
      .select(dimMetaDf.columns
        .filter(x => !(dimCol ++ metaColumns).map(x => "dim_" + x).contains(x))
        .map(x => col(x)): _*)

    val lbl_encode_df = lblencodeColumns.foldLeft(dimConcatDf) { (df, col) =>
      val indexer =
        new StringIndexer().setInputCol(col).setOutputCol(col + "_index").fit(df)
      indexer.transform(df)
    }
    //dimConcatDf.show()

    //lbl_encode_df.printSchema()

    val sorted_df = lbl_encode_df
      .withColumn(
        "dimension_comb",
        concat_ws("_", sort_array(split(col("dimension_comb"), "-"))))
      .withColumn(
        "hash_dimension_comb",
        concat_ws("_", sort_array(split(col("hash_dimension_comb"), "-"))))
    val one_hot_cols = List(
      "dimension_comb",
      "age_band_ky_",
      "flsa_status_",
      "is_manager",
      "nbr_of_diments")
    /*val one_hot_df = oneHotColumns.foldLeft(sorted_df) {(df,col) =>
      val dimenlist = col._2 match {
        case Some(y) => y.text.split(",")
        case None => df.map(r => r.getAs[Any](col._1)).distinct().collect().filter(x=>x!=null)
      }
      val groupDf = df.groupBy("ins_hash_val").pivot(col._1, dimenlist).agg(first("value"))
      val groupRenamedDf = dimenlist.foldLeft(groupDf.na.fill(0,groupDf.columns.toList)){(df,dimen) =>
      df.withColumnRenamed(dimen.toString(), col._1+"_"+dimen)
      }
      df.join(groupRenamedDf,Seq("ins_hash_val"))
    }*/
    val one_hot_df = one_hot_cols.foldLeft(sorted_df) { (df, col) =>
      val dimenlist =
        df.rdd.map(r => r.getAs[Any](col)).distinct().collect().filter(x => x != null)
      val groupDf = df.groupBy("ins_hash_val").pivot(col, dimenlist).agg(first("value"))
      val groupRenamedDf =
        dimenlist.foldLeft(groupDf.na.fill(0, groupDf.columns.toList)) { (df, dimen) =>
          df.withColumnRenamed(dimen.toString(), col + "_" + dimen)
        }
      df.join(groupRenamedDf, Seq("ins_hash_val"))
    }
    println("$$$$$$$$$$$$$$$$")
    one_hot_df.printSchema()
    one_hot_df.show(5, false)

  }
}
