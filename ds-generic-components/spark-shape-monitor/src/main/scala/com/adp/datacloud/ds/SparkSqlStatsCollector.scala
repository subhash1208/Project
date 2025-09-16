package com.adp.datacloud.ds

import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{ Row, SparkSession, DataFrame, Dataset }
import org.apache.spark.Aggregator
import org.apache.log4j.Logger
import org.apache.hadoop.mapred.StatisticsCollector
import org.apache.spark.sql.functions.{ lit,col,stddev,approx_count_distinct,expr }
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{ StructField, DataType }
import org.apache.spark.sql.types._


class SparkSqlStatsCollector(
    inputDf         : DataFrame,
    shapeConfig     : ShapeConfiguration, 
    partitioncolumns: List[String] = List() ) (implicit sparkSession : SparkSession) extends StatsCollector {
    
  private val logger = Logger.getLogger(getClass())
  import sparkSession.implicits._ 
  val columns  = inputDf.schema.fields.filter(r => !partitioncolumns.contains(r.name)).map(x => x.name -> x.dataType).toList.filterNot(y => y._1.contains("_SHAPE_VALDN"))
  
  // Performs stats collection from the given sql and creates a dataframe
  def collect(): DataFrame  = {
    
    val query = generateStatProfilesSQL(columns)
    
    logger.info("Sql created for collecting stats")
    logger.info(query)
    println(query)
    
    val statsDf = sparkSession.sql(query)
    
    val statsProfile = columns.map(column => column._2 match {
      
      case DoubleType|LongType|IntegerType
                          => statsDf .withColumn("DATABASE_NAME",     lit(shapeConfig.dbName))
                                     .withColumn("TABLE_NAME",        lit(shapeConfig.tableName))
                                     .withColumn("COLUMN_NAME",       lit(column._1))
                                     .withColumn("COLUMN_TYPE",       if (shapeConfig.virtualColumns.exists(x => x._1.equals(column._1))) lit("VIRTUAL") else lit("REAL"))
                                     .withColumn("COLUMN_SPEC",       shapeConfig.virtualColumns.filter(f => f._1.equals(column._1)) match {case v if !v.isEmpty => lit(v.head._2)
                                                                                                                                            case _               => lit(null)     })
                                     .withColumn("COLUMN_DATATYPE",   lit(column._2.toString()))
                                     .withColumn("PARTITION_SPEC",    if (!partitioncolumns.isEmpty) lit(partitioncolumns.head) else lit(null).cast("String"))
                                     .withColumn("PARTITION_VALUE",   if (!partitioncolumns.isEmpty) col(partitioncolumns.head) else lit(null).cast("String"))
                                     .withColumn("SUBPARTITION_SPEC", if (!partitioncolumns.isEmpty & partitioncolumns.length > 1) lit(partitioncolumns.last) else lit(null).cast("String"))
                                     .withColumn("SUBPARTITION_VALUE",if (!partitioncolumns.isEmpty & partitioncolumns.length > 1) col(partitioncolumns.last) else lit(null).cast("String"))
                                     .withColumn("REC_CRT_DT", lit(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date)))
                                     .withColumn(s"${column._1}_NUM_INVALID",col(s"${column._1}_VAL_INVALID"))
                                     .withColumn("APPLICATION_ID",    lit(sparkSession.sparkContext.applicationId))
                                     .select(col("APPLICATION_ID"),
                                             col("DATABASE_NAME"),
                                             col("TABLE_NAME"),
                                             col("COLUMN_NAME"),
                                             col("COLUMN_TYPE"),
                                             col("COLUMN_SPEC"),
                                             col("COLUMN_DATATYPE"),
                                             col("PARTITION_SPEC"),
                                             col("PARTITION_VALUE"),
                                             col("SUBPARTITION_SPEC"),
                                             col("SUBPARTITION_VALUE"),
                                             col("NUM_RECORDS"),
                                             col(column._1+"_NUM_MISSING").alias("NUM_MISSING"),
                                             col(column._1+"_NUM_INVALID").alias("NUM_INVALID"),
                                             col(column._1+"_AVERAGE").alias("MEAN"),
                                             col(column._1+"_STANDARD_DEVIATION").alias("STANDARD_DEVIATION"),
                                             col(column._1+"_MIN").alias("MIN"),
                                             col(column._1+"_MEDIAN").alias("MEDIAN"),
                                             col(column._1+"_FIFTH_PCTL").alias("FIFTH_PERCENTILE"),
                                             col(column._1+"_FIRST_DECILE").alias("FIRST_DECILE"),
                                             col(column._1+"_FIRST_QRTL").alias("FIRST_QUARTILE"),
                                             col(column._1+"_THIRD_QRTL").alias("THIRD_QUARTILE"),
                                             col(column._1+"_NINTH_DECILE").alias("NINTH_DECILE"),
                                             col(column._1+"_NINTY_FIFTH_PCTL").alias("NINTY_FIFTH_PERCENTILE"),
                                             col(column._1+"_MAX").alias("MAX"),
                                             col(column._1+"_APPX_UNIQUE_VALUES").alias("NUM_APPROX_UNIQUE_VALUES"),
                                             col(column._1+"_DISTINCTNESS").alias("DISTINCTNESS"),
                                             col(column._1+"_ENTROPY").alias("ENTROPY"),
                                             col(column._1+"_UNIQUE_VALUE_RATIO").alias("UNIQUE_VALUE_RATIO"),
                                             col("REC_CRT_DT"))
                                             
      case MapType(_,_,_)| ArrayType(_,_) | StructType(_)
                           => statsDf.withColumn("DATABASE_NAME",     lit(shapeConfig.dbName))
                                     .withColumn("TABLE_NAME",        lit(shapeConfig.tableName))
                                     .withColumn("COLUMN_NAME",       lit(column._1))
                                     .withColumn("COLUMN_TYPE",       if (shapeConfig.virtualColumns.exists(x => x._1.equals(column._1))) lit("VIRTUAL") else lit("REAL"))
                                     .withColumn("COLUMN_SPEC",       shapeConfig.virtualColumns.filter(f => f._1.equals(column._1)) match {case v if !v.isEmpty => lit(v.head._2)
                                                                                                                                            case _               => lit(null)     })
                                     .withColumn("COLUMN_DATATYPE",   lit(column._2.toString()))
                                     .withColumn("PARTITION_SPEC",    if (!partitioncolumns.isEmpty) lit(partitioncolumns.head) else lit(null).cast("String"))
                                     .withColumn("PARTITION_VALUE",   if (!partitioncolumns.isEmpty) col(partitioncolumns.head) else lit(null).cast("String"))
                                     .withColumn("SUBPARTITION_SPEC", if (!partitioncolumns.isEmpty & partitioncolumns.length > 1) lit(partitioncolumns.last) else lit(null).cast("String"))
                                     .withColumn("SUBPARTITION_VALUE",if (!partitioncolumns.isEmpty & partitioncolumns.length > 1) col(partitioncolumns.last) else lit(null).cast("String"))
                                     .withColumn("MEAN", lit(null))
                                     .withColumn("STANDARD_DEVIATION", lit(null))
                                     .withColumn("MIN", lit(null))
                                     .withColumn("MEDIAN", lit(null))
                                     .withColumn("FIFTH_PERCENTILE", lit(null))
                                     .withColumn("FIRST_DECILE", lit(null))
                                     .withColumn("FIRST_QUARTILE", lit(null))
                                     .withColumn("THIRD_QUARTILE", lit(null))
                                     .withColumn("NINTH_DECILE", lit(null))
                                     .withColumn("NINTY_FIFTH_PERCENTILE", lit(null))
                                     .withColumn("MAX", lit(null))
                                     .withColumn("REC_CRT_DT", lit(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date)))
                                     .withColumn(s"${column._1}_NUM_INVALID",col(s"${column._1}_VAL_INVALID"))
                                     .withColumn("NUM_APPROX_UNIQUE_VALUES",lit(0))
                                     .withColumn("DISTINCTNESS",lit(0))
                                     .withColumn("ENTROPY",lit(0))
                                     .withColumn("UNIQUE_VALUE_RATIO",lit(0))
                                     .withColumn("APPLICATION_ID",    lit(sparkSession.sparkContext.applicationId))
                                     .select(col("APPLICATION_ID"),
                                             col("DATABASE_NAME"),
                                             col("TABLE_NAME"),
                                             col("COLUMN_NAME"),
                                             col("COLUMN_TYPE"),
                                             col("COLUMN_SPEC"),
                                             col("COLUMN_DATATYPE"),
                                             col("PARTITION_SPEC"),
                                             col("PARTITION_VALUE"),
                                             col("SUBPARTITION_SPEC"),
                                             col("SUBPARTITION_VALUE"),
                                             col("NUM_RECORDS"),
                                             col(column._1+"_NUM_MISSING").alias("NUM_MISSING"),
                                             col(column._1+"_NUM_INVALID").alias("NUM_INVALID"),
                                             col("MEAN"),
                                             col("STANDARD_DEVIATION"),
                                             col("MIN"),
                                             col("MEDIAN"),
                                             col("FIFTH_PERCENTILE"),
                                             col("FIRST_DECILE"),
                                             col("FIRST_QUARTILE"),
                                             col("THIRD_QUARTILE"),
                                             col("NINTH_DECILE"),
                                             col("NINTY_FIFTH_PERCENTILE"),
                                             col("MAX"),
                                             col("NUM_APPROX_UNIQUE_VALUES"),
                                             col("DISTINCTNESS"),
                                             col("ENTROPY"),
                                             col("UNIQUE_VALUE_RATIO"),
                                             col("REC_CRT_DT"))
                                             
      case _               => statsDf.withColumn("DATABASE_NAME",     lit(shapeConfig.dbName))
                                     .withColumn("TABLE_NAME",        lit(shapeConfig.tableName))
                                     .withColumn("COLUMN_NAME",       lit(column._1))
                                     .withColumn("COLUMN_TYPE",       if (shapeConfig.virtualColumns.exists(x => x._1.equals(column._1))) lit("VIRTUAL") else lit("REAL"))
                                     .withColumn("COLUMN_SPEC",       shapeConfig.virtualColumns.filter(f => f._1.equals(column._1)) match {case v if !v.isEmpty => lit(v.head._2)
                                                                                                                                            case _               => lit(null)     })
                                     .withColumn("COLUMN_DATATYPE",   lit(column._2.toString()))
                                     .withColumn("PARTITION_SPEC",    if (!partitioncolumns.isEmpty) lit(partitioncolumns.head) else lit(null).cast("String"))
                                     .withColumn("PARTITION_VALUE",   if (!partitioncolumns.isEmpty) col(partitioncolumns.head) else lit(null).cast("String"))
                                     .withColumn("SUBPARTITION_SPEC", if (!partitioncolumns.isEmpty & partitioncolumns.length > 1) lit(partitioncolumns.last) else lit(null).cast("String"))
                                     .withColumn("SUBPARTITION_VALUE",if (!partitioncolumns.isEmpty & partitioncolumns.length > 1) col(partitioncolumns.last) else lit(null).cast("String"))
                                     .withColumn("MEAN", lit(null))
                                     .withColumn("STANDARD_DEVIATION", lit(null))
                                     .withColumn("MIN", lit(null))
                                     .withColumn("MEDIAN", lit(null))
                                     .withColumn("FIFTH_PERCENTILE", lit(null))
                                     .withColumn("FIRST_DECILE", lit(null))
                                     .withColumn("FIRST_QUARTILE", lit(null))
                                     .withColumn("THIRD_QUARTILE", lit(null))
                                     .withColumn("NINTH_DECILE", lit(null))
                                     .withColumn("NINTY_FIFTH_PERCENTILE", lit(null))
                                     .withColumn("MAX", lit(null))
                                     .withColumn("REC_CRT_DT", lit(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date)))
                                     .withColumn(s"${column._1}_NUM_INVALID",col(s"${column._1}_VAL_INVALID"))
                                     .withColumn("APPLICATION_ID",    lit(sparkSession.sparkContext.applicationId))
                                     .select(col("APPLICATION_ID"),
                                             col("DATABASE_NAME"),
                                             col("TABLE_NAME"),
                                             col("COLUMN_NAME"),
                                             col("COLUMN_TYPE"),
                                             col("COLUMN_SPEC"),
                                             col("COLUMN_DATATYPE"),
                                             col("PARTITION_SPEC"),
                                             col("PARTITION_VALUE"),
                                             col("SUBPARTITION_SPEC"),
                                             col("SUBPARTITION_VALUE"),
                                             col("NUM_RECORDS"),
                                             col(column._1+"_NUM_MISSING").alias("NUM_MISSING"),
                                             col(column._1+"_NUM_INVALID").alias("NUM_INVALID"),
                                             col("MEAN"),
                                             col("STANDARD_DEVIATION"),
                                             col("MIN"),
                                             col("MEDIAN"),
                                             col("FIFTH_PERCENTILE"),
                                             col("FIRST_DECILE"),
                                             col("FIRST_QUARTILE"),
                                             col("THIRD_QUARTILE"),
                                             col("NINTH_DECILE"),
                                             col("NINTY_FIFTH_PERCENTILE"),
                                             col("MAX"),
                                             col(column._1+"_APPX_UNIQUE_VALUES").alias("NUM_APPROX_UNIQUE_VALUES"),
                                             col(column._1+"_DISTINCTNESS").alias("DISTINCTNESS"),
                                             col(column._1+"_ENTROPY").alias("ENTROPY"),
                                             col(column._1+"_UNIQUE_VALUE_RATIO").alias("UNIQUE_VALUE_RATIO"),
                                             col("REC_CRT_DT"))  })
    
    val initialDF = Seq.empty[ColumnStatistics].toDF()
    val statsData = statsProfile.foldLeft(initialDF)(_ union _)
 
    statsData
  }
  
  // This function creates sqls with all the grouping sets available and transform it in a dataframe of the desired structure
  // It also persist the data for further use 
  def generateStatProfilesSQL(columns: List[(String, DataType)]) (implicit sparkSession : SparkSession): String = {

    sparkSession.udf.register("entropy", new EntropyUdf)
    sparkSession.udf.register("unique_value_ratio", new UniqueValRatio)
    
    val columnExprs = columns.map(x => x._2 match {
      
      case DoubleType | LongType | IntegerType
                             => s""" avg(${x._1})                                            as ${x._1}_AVERAGE, 
                                     sum(case when ${x._1} is null then 1 else 0 end)        as ${x._1}_NUM_MISSING,
                                     sum(${if (shapeConfig.columnValidations.exists(y=> y.columnName.equals(x._1)))
                                             shapeConfig.columnValidations.filter(y => y.columnName.equals(x._1))
                                                                          .head
                                                                          .validationExpr match{
                                       case None       =>  new String("case when 1=0 then 1 else 0 end")
                                       case Some(expr) =>  expr }
                                           else "0"} )                                       as ${x._1}_VAL_INVALID,
                                     stddev(${x._1})                                         as ${x._1}_STANDARD_DEVIATION,
                                     min(${x._1})                                            as ${x._1}_MIN,
                                     max(${x._1})                                            as ${x._1}_MAX,
                                     approx_percentile(${x._1},0.05, 5000)                   as ${x._1}_FIFTH_PCTL,
                                     approx_percentile(${x._1},0.1, 5000)                    as ${x._1}_FIRST_DECILE, 
                                     approx_percentile(${x._1},0.25, 5000)                   as ${x._1}_FIRST_QRTL,
                                     approx_percentile(${x._1},0.5, 5000)                    as ${x._1}_MEDIAN,
                                     approx_percentile(${x._1},0.75, 5000)                   as ${x._1}_THIRD_QRTL,
                                     approx_percentile(${x._1},0.9, 5000)                    as ${x._1}_NINTH_DECILE,
                                     approx_percentile(${x._1},0.95, 5000)                   as ${x._1}_NINTY_FIFTH_PCTL,
                                     approx_count_distinct(${x._1})                          as ${x._1}_APPX_UNIQUE_VALUES, 
                                     approx_count_distinct(${x._1}) /count(1)                as ${x._1}_DISTINCTNESS,
                                     entropy(${x._1})                                        as ${x._1}_ENTROPY,
                                     unique_value_ratio(${x._1})                             as ${x._1}_UNIQUE_VALUE_RATIO"""
                                     
      case MapType(_,_,_) | ArrayType(_,_) | StructType(_)
                             => s""" sum(case when ${x._1} is null then 1 else 0 end)        as ${x._1}_NUM_MISSING,
                                     sum(${if (shapeConfig.columnValidations.exists(y=> y.columnName.equals(x._1)))
                                             shapeConfig.columnValidations.filter(y => y.columnName.equals(x._1))
                                                                          .head
                                                                          .validationExpr match{
                                       case None       =>  new String("case when 1=0 then 1 else 0 end")
                                       case Some(expr) =>  expr }
                                           else "0"} )                                       as ${x._1}_VAL_INVALID"""
                                     
      case _                 => s""" sum(case when ${x._1} is null then 1 else 0 end)        as ${x._1}_NUM_MISSING,
                                     sum(${if (shapeConfig.columnValidations.exists(y=> y.columnName.equals(x._1)))
                                             shapeConfig.columnValidations.filter(y => y.columnName.equals(x._1))
                                                                          .head
                                                                          .validationExpr match{
                                       case None       =>  new String("case when 1=0 then 1 else 0 end")
                                       case Some(expr) =>  expr }
                                           else "0"} )                                       as ${x._1}_VAL_INVALID, 
                                     count(distinct ${x._1})                                 as ${x._1}_APPX_UNIQUE_VALUES,
                                     count(distinct ${x._1}) /count(1)                       as ${x._1}_DISTINCTNESS,
                                     entropy(${x._1})                                        as ${x._1}_ENTROPY,
                                     unique_value_ratio(${x._1})                             as ${x._1}_UNIQUE_VALUE_RATIO"""  })
    
    inputDf.createOrReplaceTempView("hiveDf")
     
    if (partitioncolumns.isEmpty) {
      s"""SELECT count(1) as NUM_RECORDS, ${columnExprs.mkString(",")} FROM hiveDf """
    } else {
      val groupingSets    = partitioncolumns.toSet.subsets().map(_.toList).filter(x => x.contains(partitioncolumns.head) & x.length > 1).toList
      val groupingSetExpr = groupingSets.mkString(",").replaceAll("List", "")
       
      s"""SELECT 
            count(1) as NUM_RECORDS, ${columnExprs.mkString(",")} ,${partitioncolumns.mkString(",")} 
          FROM hiveDf 
          GROUP BY ${partitioncolumns.mkString(",")}
          GROUPING SETS (${groupingSetExpr})"""
      }
    }
  
}
