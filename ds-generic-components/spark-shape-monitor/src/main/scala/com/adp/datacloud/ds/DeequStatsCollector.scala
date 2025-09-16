package com.adp.datacloud.ds

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.parallel.ForkJoinTaskSupport

class DeequStatsCollector(
                           df: DataFrame,
                           shapeConfig: ShapeConfiguration,
                           partitioncolumns: Option[List[String]] = None
                         )(implicit sparkSession: SparkSession)
  extends StatsCollector {

  val partitioncol = partitioncolumns.getOrElse(List())
  import sparkSession.sqlContext.implicits._
  val columns = df.schema.fields
    .filter(r => !partitioncol.contains(r.name))
    .map(x => x.name -> x.dataType)
    .toList
    .filterNot(y => y._1.contains("_SHAPE_VALDN"))
  private val logger = Logger.getLogger(getClass())

  // Performs stats collection on the dataframe using AWS Deequ
  def collect(): DataFrame = {

    val columnAnalyzers = columns.flatMap(x =>
      x match {

        case a
          if (a._2.isInstanceOf[DoubleType] |
            a._2.isInstanceOf[LongType] |
            a._2.isInstanceOf[IntegerType]) =>
          logger.info(s"adding Numeric type Analyzers the column ${x._1} of type ${x._2}")
          Seq(
            Completeness(a._1),
            Mean(a._1),
            StandardDeviation(a._1),
            Minimum(a._1),
            ApproxQuantiles(a._1, Seq(0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95)),
            Maximum(a._1),
            ApproxCountDistinct(a._1),
            Distinctness(a._1),
            Entropy(a._1),
            UniqueValueRatio(a._1),
            Sum(a._1 + "_SHAPE_VALDN")
          )

        case x
          if (x._2.isInstanceOf[MapType] |
            x._2.isInstanceOf[ArrayType] |
            x._2.isInstanceOf[StructType]) =>
          logger.info(s"adding Collection type Analyzers the column ${x._1} of type ${x._2}")
          Seq(Completeness(x._1), Sum(x._1 + "_SHAPE_VALDN"))

        case b =>
          logger.info(s"adding default Analyzers the column ${x._1} of type ${x._2}")
          Seq(
            Completeness(b._1),
            CountDistinct(b._1),
            Distinctness(b._1),
            Entropy(b._1),
            UniqueValueRatio(b._1),
            Sum(b._1 + "_SHAPE_VALDN")
          )
      }
    )

    val allAnalyzerContext: List[(List[String], AnalyzerContext)] = {
      if (!partitioncol.isEmpty) {
        val partitionCombos = getPartitionCombos(df)
        val filterExpr = partitionCombos.map(_.reduce(_ + " and " + _)).par
        filterExpr.tasksupport = new ForkJoinTaskSupport(
          new scala.concurrent.forkjoin.ForkJoinPool(10)
        )

        partitionCombos.zip(
          (for (filter <- filterExpr)
            yield AnalysisRunner
              .onData(df.filter(filter))
              .addAnalyzers(columnAnalyzers)
              .addAnalyzer(Size())
              .run()).toList
        )
      } else {
        List(
          (
            List(),
            AnalysisRunner
              .onData(df)
              .addAnalyzers(columnAnalyzers)
              .addAnalyzer(Size())
              .run()
          )
        )
      }
    }

    val allMetrics = allAnalyzerContext.map(x =>
      (x._1, successMetricsAsDataFrame(sparkSession, x._2))
    )
    val stats = allMetrics
      .map(x =>
        x._2
          .withColumn(
            "PARTITION_SPEC",
            lit(x match {
              case p if !p._1.isEmpty => p._1(0).split(" = ").head.trim
              case _                  => null
            }).cast("String")
          )
          .withColumn(
            "PARTITION_VALUE",
            lit(x match {
              case p if !p._1.isEmpty =>
                p._1(0).split(" = ").last.replace("\'", "").trim
              case _ => null
            }).cast("String")
          )
          .withColumn(
            "SUBPARTITION_SPEC",
            lit(x match {
              case p if p._1.length > 1 => p._1(1).split(" = ").head.trim
              case _                    => null
            }).cast("String")
          )
          .withColumn(
            "SUBPARTITION_VALUE",
            lit(x match {
              case p if p._1.length > 1 =>
                p._1(1).split(" = ").last.replace("\'", "").trim
              case _ => null
            }).cast("String")
          )
      )
      .reduce(_ union _)

    stats.show(100, false)
    stats.printSchema
    columns.foreach(x => print(x._1, " ", x._2, "::"))
// Median and percentile fixes
    val dataFrame = columns.map(x =>
      x._2 match {

        case DoubleType | LongType | IntegerType => {
          logger.info(s"Adjusting DF for the column ${x._1} of type ${x._2}")
          val statsFrame = stats
            .filter(
              s"instance = '${x._1}' or instance = '*' or instance ='${x._1}_SHAPE_VALDN'"
            )
            .withColumn("COLUMN_NAME", lit(x._1))
            .withColumn(
              "name2",
              expr(
                """case when name ='ApproxQuantiles-0.25' then 'name025'
                                                                                        when name ='ApproxQuantiles-0.5'  then 'name05'
                                                                                        when name ='ApproxQuantiles-0.75' then 'name075'
                                                                                        when name ='ApproxQuantiles-0.05' then 'name005'
                                                                                        when name ='ApproxQuantiles-0.1' then 'name01'
                                                                                        when name ='ApproxQuantiles-0.9' then 'name09'
                                                                                        when name ='ApproxQuantiles-0.95' then 'name095'
                                                                                        else name end"""
              )
            )
            .drop("name")
            .groupBy(
              "COLUMN_NAME",
              "PARTITION_SPEC",
              "PARTITION_VALUE",
              "SUBPARTITION_SPEC",
              "SUBPARTITION_VALUE"
            )
            .pivot("name2")
            .agg(max("value"))

          val validColumns = statsFrame.columns.toSeq
          val validColumns2 = stats.columns.toSeq

          statsFrame.show(100, false)
          statsFrame.printSchema


          statsFrame
            .withColumn("DATABASE_NAME", lit(shapeConfig.dbName))
            .withColumn("TABLE_NAME", lit(shapeConfig.tableName))
            .withColumn(
              "COLUMN_TYPE",
              if (shapeConfig.virtualColumns.exists(y => y._1.equals(x._1)))
                lit("VIRTUAL")
              else lit("REAL")
            )
            .withColumn(
              "COLUMN_SPEC",
              shapeConfig.virtualColumns.filter(f => f._1.equals(x._1)) match {
                case v if !v.isEmpty => lit(v.head._2)
                case _               => lit(null)
              }
            )
            .withColumn("COLUMN_DATATYPE", lit(x._2.toString()))
            .withColumn(
              "REC_CRT_DT",
              lit(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date))
            )
            .withColumn(
              "APPLICATION_ID",
              lit(sparkSession.sparkContext.applicationId)
            )
            .withColumn(
              "MEAN",
              validColumns.contains("Mean") match {
                case true  => coalesce(expr("Mean"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "FIFTH_PERCENTILE",
              validColumns.contains("name005") match {
                case true =>
                  coalesce(col("name005"), lit(0)) /*col("name-0.25")*/
                case false => lit(0)
              }
            )
            .withColumn(
              "FIRST_DECILE",
              validColumns.contains("name01") match {
                case true =>
                  coalesce(col("name01"), lit(0)) /*col("name-0.25")*/
                case false => lit(0)
              }
            )
            .withColumn(
              "FIRST_QUARTILE",
              validColumns.contains("name025") match {
                case true =>
                  coalesce(col("name025"), lit(0)) /*col("name-0.25")*/
                case false => lit(0)
              }
            )
            .withColumn(
              "MEDIAN",
              validColumns.contains("name05") match {
                case true  => coalesce(col("name05"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "THIRD_QUARTILE",
              validColumns.contains("name075") match {
                case true  => coalesce(col("name075"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "NINTH_DECILE",
              validColumns.contains("name09") match {
                case true  => coalesce(col("name09"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "NINTY_FIFTH_PERCENTILE",
              validColumns.contains("name095") match {
                case true  => coalesce(col("name095"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "STANDARD_DEVIATION",
              validColumns.contains("StandardDeviation") match {
                case true  => coalesce(col("StandardDeviation"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "MIN",
              validColumns.contains("Minimum") match {
                case true  => coalesce(col("Minimum"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "MAX",
              validColumns.contains("Maximum") match {
                case true  => coalesce(col("Maximum"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "UNIQUE_VALUE_RATIO",
              validColumns.contains("UniqueValueRatio") match {
                case true  => coalesce(col("UniqueValueRatio"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "DISTINCTNESS",
              validColumns.contains("Distinctness") match {
                case true  => coalesce(col("Distinctness"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "ENTROPY",
              validColumns.contains("Entropy") match {
                case true  => coalesce(col("Entropy"), lit(0))
                case false => lit(0)
              }
            )
            //adding completeness column in stats dataframe to derive num_missing metric. Spark 3.2 and deequ incompatibility fix[1]
            .withColumn(
              "COMPLETENESS",
              validColumns.contains("Completeness") match {
                case true  => coalesce(col("Completeness"), lit(0))
                case false => lit(0)
              }
            )
            .withColumnRenamed("Size", "NUM_RECORDS")
            .withColumnRenamed(
              "ApproxCountDistinct",
              "NUM_APPROX_UNIQUE_VALUES"
            )
            .withColumn(
              "NUM_MISSING",
              (col("NUM_RECORDS") - col("Completeness") * col("NUM_RECORDS"))
            )
            .withColumn(
              "NUM_INVALID",
              if (
                shapeConfig.columnValidations
                  .exists(y => y.columnName.equals(x._1))
              )
                shapeConfig.columnValidations
                  .filter(y => y.columnName.equals(x._1))
                  .head
                  .validationExpr match {
                  case Some(x) => coalesce(col("sum"), lit(0))
                  case None    => lit(0)
                }
              else lit(0)
            )
            .select(
              col("APPLICATION_ID"),
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
              col("NUM_MISSING"),
              col("NUM_INVALID"),
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
              col("REC_CRT_DT")
            )
        }

        case MapType(_, _, _) | ArrayType(_, _) | StructType(_) =>{
          logger.info(s"Adjusting DF for the column ${x._1} of type ${x._2}")
          stats
            .filter(
              s"instance = '${x._1}' or instance = '*' or instance ='${x._1}_SHAPE_VALDN'"
            )
            .withColumn("COLUMN_NAME", lit(x._1))
            .groupBy(
              "COLUMN_NAME",
              "PARTITION_SPEC",
              "PARTITION_VALUE",
              "SUBPARTITION_SPEC",
              "SUBPARTITION_VALUE"
            )
            .pivot("name")
            .agg(max("value"))
            .withColumn("DATABASE_NAME", lit(shapeConfig.dbName))
            .withColumn("TABLE_NAME", lit(shapeConfig.tableName))
            .withColumn(
              "COLUMN_TYPE",
              if (shapeConfig.virtualColumns.exists(y => y._1.equals(x._1)))
                lit("VIRTUAL")
              else lit("REAL")
            )
            .withColumn(
              "COLUMN_SPEC",
              shapeConfig.virtualColumns.filter(f => f._1.equals(x._1)) match {
                case v if !v.isEmpty => lit(v.head._2)
                case _               => lit(null)
              }
            )
            .withColumn("COLUMN_DATATYPE", lit(x._2.toString()))
            .withColumn(
              "REC_CRT_DT",
              lit(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date))
            )
            .withColumn(
              "APPLICATION_ID",
              lit(sparkSession.sparkContext.applicationId)
            )
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
            .withColumn("NUM_APPROX_UNIQUE_VALUES", lit(0))
            .withColumnRenamed("Size", "NUM_RECORDS")
            .withColumn("UNIQUE_VALUE_RATIO", lit(0))
            .withColumn("COMPLETENESS", lit(0))
            .withColumn(
              "NUM_MISSING",
              (col("NUM_RECORDS") - col("COMPLETENESS") * col("NUM_RECORDS"))
            )
            .withColumn(
              "NUM_INVALID",
              if (
                shapeConfig.columnValidations
                  .exists(y => y.columnName.equals(x._1))
              )
                shapeConfig.columnValidations
                  .filter(y => y.columnName.equals(x._1))
                  .head
                  .validationExpr match {
                  case Some(x) => coalesce(col("sum"), lit(0))
                  case None    => lit(0)
                }
              else lit(0)
            )
            .withColumn("DISTINCTNESS", lit(0))
            .withColumn("ENTROPY", lit(0))
            .select(
              col("APPLICATION_ID"),
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
              col("NUM_MISSING"),
              col("NUM_INVALID"),
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
              col("REC_CRT_DT")
            )}

        case _ => {
          logger.info(s"Adjusting DF for the column ${x._1} of type ${x._2}")
          val statsFrame = stats
            .filter(
              s"instance = '${x._1}' or instance = '*' or instance ='${x._1}_SHAPE_VALDN'"
            )
            .withColumn("COLUMN_NAME", lit(x._1))
            .groupBy(
              "COLUMN_NAME",
              "PARTITION_SPEC",
              "PARTITION_VALUE",
              "SUBPARTITION_SPEC",
              "SUBPARTITION_VALUE"
            )
            .pivot("name")
            .agg(max("value"))

          val validColumns = statsFrame.columns.toSeq

          stats
            .filter(
              s"instance = '${x._1}' or instance = '*' or instance ='${x._1}_SHAPE_VALDN'"
            )
            .withColumn("COLUMN_NAME", lit(x._1))
            .groupBy(
              "COLUMN_NAME",
              "PARTITION_SPEC",
              "PARTITION_VALUE",
              "SUBPARTITION_SPEC",
              "SUBPARTITION_VALUE"
            )
            .pivot("name")
            .agg(max("value"))
            .withColumn("DATABASE_NAME", lit(shapeConfig.dbName))
            .withColumn("TABLE_NAME", lit(shapeConfig.tableName))
            .withColumn(
              "COLUMN_TYPE",
              if (shapeConfig.virtualColumns.exists(y => y._1.equals(x._1)))
                lit("VIRTUAL")
              else lit("REAL")
            )
            .withColumn(
              "COLUMN_SPEC",
              shapeConfig.virtualColumns.filter(f => f._1.equals(x._1)) match {
                case v if !v.isEmpty => lit(v.head._2)
                case _               => lit(null)
              }
            )
            .withColumn("COLUMN_DATATYPE", lit(x._2.toString()))
            .withColumn(
              "REC_CRT_DT",
              lit(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date))
            )
            .withColumn(
              "APPLICATION_ID",
              lit(sparkSession.sparkContext.applicationId)
            )
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
            .withColumnRenamed("Size", "NUM_RECORDS")
            .withColumnRenamed("CountDistinct", "NUM_APPROX_UNIQUE_VALUES")
            .withColumn(
              "UNIQUE_VALUE_RATIO",
              validColumns.contains("UniqueValueRatio") match {
                case true  => coalesce(col("UniqueValueRatio"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "DISTINCTNESS",
              validColumns.contains("Distinctness") match {
                case true  => coalesce(col("Distinctness"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "ENTROPY",
              validColumns.contains("Entropy") match {
                case true  => coalesce(col("Entropy"), lit(0))
                case false => lit(0)
              }
            )
            //adding completeness column in stats dataframe to derive num_missing metric. Spark 3.2 and deequ incompatibility fix[1]
            .withColumn(
              "COMPLETENESS",
              validColumns.contains("Completeness") match {
                case true  => coalesce(col("Completeness"), lit(0))
                case false => lit(0)
              }
            )
            .withColumn(
              "NUM_MISSING",
              (col("NUM_RECORDS") - col("Completeness") * col("NUM_RECORDS"))
            )
            .withColumn(
              "NUM_INVALID",
              if (
                shapeConfig.columnValidations
                  .exists(y => y.columnName.equals(x._1))
              )
                shapeConfig.columnValidations
                  .filter(y => y.columnName.equals(x._1))
                  .head
                  .validationExpr match {
                  case Some(x) => coalesce(col("sum"), lit(0))
                  case None    => lit(0)
                }
              else lit(0)
            )
            .select(
              col("APPLICATION_ID"),
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
              col("NUM_MISSING"),
              col("NUM_INVALID"),
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
              col("REC_CRT_DT")
            )
        }
      }
    )

    val initialDF = Seq.empty[ColumnStatistics].toDF()
    val statsData = dataFrame.foldLeft(initialDF)(_ union _)

    statsData
  }

  /*
   * This function generates all the possible partition combinations available in the input DataFrame
   *
   * */
  def getPartitionCombos(df: DataFrame): List[List[String]] = {

    partitioncolumns match {
      case Some(pcolumns) =>
        df.rollup(pcolumns.map(c => col(c)): _*)
          .count()
          .filter(s"${pcolumns.head} is not null")
          .drop("count")
          .collect
          .map(
            _.toSeq
              .map(x =>
                x match {
                  case x if x.isInstanceOf[Any]  => x.toString()
                  case x if !x.isInstanceOf[Any] => "-1"
                }
              )
          )
          .map(x =>
            for {
              i <- 0 until x.size
              j <- 0 until pcolumns.length
            } yield {
              if (i == j & !x(i).equals("-1")) s"${pcolumns(j)} = '${x(i)}'"
              else s""
            }
          )
          .map(_.toList.filter(!_.isEmpty()))
          .toList
      case None => null
    }
  }
}