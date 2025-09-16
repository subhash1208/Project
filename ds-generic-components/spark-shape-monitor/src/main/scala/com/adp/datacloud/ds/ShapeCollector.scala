package com.adp.datacloud.ds

import com.adp.datacloud.cli.ShapeMonitorConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.{PrintWriter, StringWriter}

case class ExecutionLog(
    tableName: String,
    step: String,
    is_error: Boolean = false,
    error_msg: String = null)

class ShapeCollector {

  private val logger = Logger.getLogger(getClass())

  //Global list of time partitions for reference
  val monthColumns = List("month", "yyyymm", "ingest_month")
  val qtrColumns   = List("qtr", "qtr_cd", "quarter")

  def statsComputationDriver(
      shapeConfiguration: ShapeConfiguration,
      ShapeMonitorConfig: ShapeMonitorConfig,
      testMode: Boolean = false)(implicit sparkSession: SparkSession): ExecutionLog = {

    val dbname        = shapeConfiguration.dbName
    val tablename     = shapeConfiguration.tableName
    val executionStep = "ShapesCollection"

    try {

      val inputDataFrame = sparkSession.table(dbname + "." + tablename)

      // Setup Virtual Columns and drop Exclude Columns
      val inputDf = shapeConfiguration.virtualColumns
        .foldLeft(inputDataFrame)({ (df, column) =>
          df.withColumn(column._1, expr(column._2))
        })
        .drop(shapeConfiguration.excludeColumns: _*)

      // Adding additional columns with computed row level shape validation rules
      val transformedInputDf = shapeConfiguration.columnValidations
        .foldLeft(inputDf)({ (df, column) =>
          column.validationExpr match {
            case Some(x) => df.withColumn(s"${column.columnName}_SHAPE_VALDN", expr(x))
            case None    => df
          }
        })

      // Normalize monthColumns. Rename anything related to "yyyymm"
      // Assumes that only one of the monthColumns is present in a table which should be the case in all ADP tables
      val monthNormalizedInputDf = monthColumns
        .diff(List("yyyymm"))
        .intersect(transformedInputDf.columns)
        .foldLeft(transformedInputDf)({ (df, column) =>
          df.withColumn("yyyymm", col(column)).drop(col(column))
        })

      // Normalize qtrColumns. Rename anything related to "qtr"
      val qtrNormalizedInputDf = qtrColumns
        .diff(List("qtr"))
        .intersect(transformedInputDf.columns)
        .foldLeft(monthNormalizedInputDf)({ (df, column) =>
          df.withColumn("qtr", col(column)).drop(col(column))
        })

      // Hacky way. If a table consists of both yyyymm and qtr, we will consider the former as the partition column
      val temporalPartitionColumns =
        qtrNormalizedInputDf.columns.intersect(List("yyyymm", "qtr")).toList.take(1)

      val partitionColumns =
        (temporalPartitionColumns ++ shapeConfiguration.partitionColumns).toSet.toList

      val inputStgDf = (
        shapeConfiguration.analyzeColumns.isEmpty,
        shapeConfiguration.excludeColumns.isEmpty) match {

        case (true, true)  => qtrNormalizedInputDf
        case (true, false) => qtrNormalizedInputDf
        case (false, true) => {
          qtrNormalizedInputDf.printSchema
          val columns = qtrNormalizedInputDf.schema.fields
            .map(_.name)
            .diff(
              shapeConfiguration.analyzeColumns
                ++ partitionColumns
                ++ shapeConfiguration.virtualColumns.map(_._1)
                ++ shapeConfiguration.columnValidations.map(_.columnName)
                ++ qtrNormalizedInputDf.schema.fields
                  .map(_.name)
                  .filter(_.contains("_SHAPE_VALDN")))
          qtrNormalizedInputDf.drop(columns: _*)

        }
        case (false, false) => {
          println(
            "Both analyzecolumns and excludecolumns are configured. Ignoring analyze columns configuration")
          qtrNormalizedInputDf
        }

      }

      // Filter the data if statistics are already calculated for specific partitions, to enable incremental stats collection.
      val dataDf =
        if (ShapeMonitorConfig.rerun | partitionColumns.isEmpty) inputStgDf
        else {
          val computedPartitions = getComputedPartitionsDtls(
            partitionColumns,
            ShapeMonitorConfig,
            dbname,
            tablename)
          val computedPartitionsFilter = computedPartitions
            .collect()
            .map(x => List(x(0).toString(), x(1).toString()).mkString(" not in "))
            .mkString
            .replaceAll("^$", "1 = 1")
          inputStgDf.where(s"${computedPartitionsFilter}")
        }
      val inputDataDf = ShapeMonitorConfig.quickRun match {
        case true =>
          partitionColumns.size match {
            case x if x >= 1 =>
              dataDf.filter(
                s"${partitionColumns.head} = '${ShapeMonitorConfig.partitionValue.get}'")
            case _ => dataDf
          }
        case false => dataDf
      }
      if (!inputDataDf.head(1).isEmpty) {
        // Capture DeeQu stats
        val deequStatsCollector = new DeequStatsCollector(
          inputDataDf,
          shapeConfiguration,
          Option(partitionColumns.take(1)))
        val deequStatsCollectedDf = deequStatsCollector.collect()

        // Collect stats using native spark sql for total partition columns combo is more than or equal to 2,
        val statsDf = if (partitionColumns.size > 1) {
          // Use spark-sql based stats collection
          val sparkSqlStatsCollector =
            new SparkSqlStatsCollector(inputDataDf, shapeConfiguration, partitionColumns)
          val sparkSqlStatsCollectedDf = sparkSqlStatsCollector.collect()
          (sparkSqlStatsCollectedDf.union(deequStatsCollectedDf)).toDF()
        } else deequStatsCollectedDf
        // Saving collected statistics to rdbms table
        if (testMode) {
          statsDf.show(500, false)
          print(
            s"Test for $dbname.$tablename table statistics computation is completed\n")
          ExecutionLog(s"$dbname.$tablename", executionStep)
        } else {
          saveStatistics(statsDf, ShapeMonitorConfig)
          print(s"$dbname.$tablename table statistics computation is completed\n")
          ExecutionLog(s"$dbname.$tablename", executionStep)
        }
      } else if (ShapeMonitorConfig.quickRun && ShapeMonitorConfig.rerun)
        print(
          s"\n$dbname.$tablename table stats cannot be collected as specified partition data does not exist.\n")
      else if (ShapeMonitorConfig.quickRun && !ShapeMonitorConfig.rerun)
        print(
          s"\n$dbname.$tablename table stats are either collected already or the specified partition value does not exist in the dataset. Please check validations \n")
      else
        print(
          s"\n$dbname.$tablename table stats are already computed for all existing partitions. There is no new data available for this table to calculate in this job run.\n")
      ExecutionLog(s"$dbname.$tablename", executionStep)
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        print(s"$dbname.$tablename table statistics computation is failed\n")
        ExecutionLog(s"$dbname.$tablename", executionStep, true, sw.toString)
      }
    }
  }

  // Determine if any computed statistics are available for the given table to avoid recalculation for a given partition
  def getComputedPartitionsDtls(
      partitionColumns: List[String],
      shapeMonitorConfig: ShapeMonitorConfig,
      dbname: String,
      tablename: String)(implicit sparkSession: SparkSession): DataFrame = {

    val statsSql =
      s"""
         |(SELECT
         |                                partition_spec,
         |                                '(''' || LISTAGG(partition_value, ''',''') WITHIN GROUP (ORDER BY partition_value) || ''')' as partition_value
         |                              FROM
         |                                (SELECT DISTINCT
         |                                      partition_spec,
         |                                      partition_value 
         |                                 FROM ${shapeMonitorConfig.targetTable}
         |                                 WHERE LOWER(database_name)              = '${dbname}'         AND
         |                                       LOWER(table_name)                 = '${tablename}'      AND
         |                                       LOWER(partition_spec)             = '${partitionColumns.head}' AND
         |                                       NVL(LOWER(subpartition_spec),' ') = NVL('${partitionColumns.tail.mkString}',' '))
         |                              GROUP BY partition_spec ) 
         |""".stripMargin.trim

    // Read if any existing stats are already calculated for the given table
    val statsTableDf: DataFrame = sparkSession.read.jdbc(
      shapeMonitorConfig.jdbcUrl,
      statsSql,
      shapeMonitorConfig.jdbcProperties)
    statsTableDf
  }

  // Write collected Column statistics to rdbms table
  def saveStatistics(
      statisticsDataFrame: DataFrame,
      shapeMonitorConfig: ShapeMonitorConfig,
      is_validation_table: Boolean = false) {

    if (!is_validation_table) {
      statisticsDataFrame.write
        .option("numPartitions", 2)
        .option("batchsize", 30)
        .mode(SaveMode.Append)
        .jdbc(
          shapeMonitorConfig.jdbcUrl,
          s"${shapeMonitorConfig.targetTable}_STG",
          shapeMonitorConfig.jdbcProperties)

      logger.info(
        s"Statistics are successfully written to ${shapeMonitorConfig.targetTable}_STG table")
    } else {
      statisticsDataFrame.write
        .option("numPartitions", 2)
        .option("batchsize", 30)
        .mode(SaveMode.Append)
        .jdbc(
          shapeMonitorConfig.jdbcUrl,
          s"${shapeMonitorConfig.targetValidationTable}_STG",
          shapeMonitorConfig.jdbcProperties)

      logger.info(
        s"Validations are successfully written to ${shapeMonitorConfig.targetValidationTable}_STG table")
    }

  }

}
