package com.adp.datacloud.ds

import com.adp.datacloud.cli.ShapeMonitorConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Date

class ShapeValidator {

  private val logger = Logger.getLogger(getClass())

  //Global list of time partitions for reference
  val monthColumns = List("month", "yyyymm", "ingest_month")
  val qtrColumns   = List("qtr", "qtr_cd", "quarter")

  def shapeValidationDriver(
      shapeConfiguration: ShapeConfiguration,
      shapeMonitorConfig: ShapeMonitorConfig,
      testMode: Boolean = false)(implicit
      sparkSession: SparkSession): (ExecutionLog, Option[String]) = {

    val dbname         = shapeConfiguration.dbName
    val tablename      = shapeConfiguration.tableName
    val executionStep  = "ShapesValidation"
    val shapeCollector = new ShapeCollector

    try {
      val statisticsData = {
        if (testMode)
          sparkSession.read
            .parquet("src/test/resources/column_statistics")
            .filter("PARTITION_VALUE='WFN'")
            .drop("PARTITION_SPEC")
            .drop("PARTITION_VALUE")
            .withColumn("PARTITION_SPEC", lit("yyyymm"))
            .withColumn("PARTITION_VALUE", lit("201901"))
            .union(
              sparkSession.read
                .parquet("src/test/resources/column_statistics")
                .filter("PARTITION_VALUE='WFN'")
                .drop("PARTITION_SPEC")
                .drop("PARTITION_VALUE")
                .withColumn("PARTITION_SPEC", lit("yyyymm"))
                .withColumn("PARTITION_VALUE", lit("201902")))
            .withColumn("VALIDATIONS_COMPUTED", lit("No"))
        else
          fetchGeneratedStats(shapeMonitorConfig, shapeConfiguration)
      }

      //statisticsData.select("TABLE_NAME","PARTITION_SPEC","PARTITION_VALUE","VALIDATIONS_COMPUTED").distinct.show(1000,false)
      //println("printed fetched data")
      val validationsDf =
        validateShape(statisticsData, shapeConfiguration, shapeMonitorConfig)

      if (testMode) {
        //validationsDf.show(1000, false)
        print(
          s"Test For $dbname.$tablename table validation(s) are completed and saved to Validations table\n")
        (ExecutionLog(s"$dbname.$tablename", executionStep), None)
      } else {
        shapeCollector.saveStatistics(validationsDf, shapeMonitorConfig, true)
        print(
          s"$dbname.$tablename table validation(s) are completed and saved to Validations table\n")
        //validationsDf.show(1000, false)
        val validationFailureTablesList = {
          val validationFailures =
            validationsDf
              .filter(s"VALIDATION_STATUS = 'FAIL' OR NUM_INVALIDS > 0")
              .select(col("TABLE_NAME"))
              .distinct()
              .collect()
              .map(x => (x(0).toString()))
              .toList
          if (validationFailures.isEmpty) None else Some(validationFailures.mkString)
        }
        (ExecutionLog(s"$dbname.$tablename", executionStep), validationFailureTablesList)
      }
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        print(s"$dbname.$tablename table validations computation is failed\n")
        (ExecutionLog(s"$dbname.$tablename", executionStep, true, sw.toString), None)
      }
    }
  }

  def fetchGeneratedStats(
      shapeMonitorConfig: ShapeMonitorConfig,
      shapeConfiguration: ShapeConfiguration)(implicit
      sparkSession: SparkSession): DataFrame = {

    val statsQuerySql =
      s"""
         |(SELECT a.*,
         |       Cast(Dense_rank()
         |              over (
         |                PARTITION BY a.database_name, a.table_name
         |                ORDER BY a.partition_value DESC, a.subpartition_value DESC) AS
         |            INT) AS
         |       rank,
         |       CASE
         |         WHEN a.partition_spec IS NULL
         |               OR '${shapeMonitorConfig.rerun}' = 'true'
         |               OR b.partition_combo IS NULL THEN 'No'
         |         ELSE 'Yes'
         |       END
         |       AS validations_computed
         |FROM   ${shapeMonitorConfig.targetTable}  a
         |       left join (SELECT DISTINCT ( partition_value
         |                                    ||subpartition_value ) AS partition_combo
         |                  FROM   ${shapeMonitorConfig.targetValidationTable}
         |                  WHERE  database_name = '${shapeConfiguration.dbName}'
         |                         AND table_name = '${shapeConfiguration.tableName}'
         |                         AND column_name IS NOT NULL) b
         |              ON ( a.partition_value
         |                   ||a.subpartition_value ) = b.partition_combo
         |WHERE  database_name = '${shapeConfiguration.dbName}'
         |       AND table_name = '${shapeConfiguration.tableName}')
         """.stripMargin.trim

    val collectedStats = sparkSession.read
      .jdbc(shapeMonitorConfig.jdbcUrl, statsQuerySql, shapeMonitorConfig.jdbcProperties)
      .persist()

    // trigger count to force persist
    collectedStats.count()

    val finalCollectedStats = shapeMonitorConfig.quickRun match {
      case true =>
        collectedStats
          .withColumn(
            "validations_computed_2",
            expr(
              s"CASE WHEN partition_value != '${shapeMonitorConfig.partitionValue.get}' then 'Yes' else validations_computed END"))
          .drop("validations_computed")
          .withColumnRenamed("validations_computed_2", "validations_computed")
      case false => collectedStats
    }

    finalCollectedStats

  }

  def validateShape(
      df: DataFrame,
      shapeConfig: ShapeConfiguration,
      shapeMonitorConfig: ShapeMonitorConfig)(implicit
      sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._

    val aggValidationCols = shapeConfig.columnValidations
      .filter(
        y =>
          (y.maxNullPctg.isInstanceOf[Some[String]] |
            y.maxCardinality.isInstanceOf[Some[String]] |
            y.minCardinality.isInstanceOf[Some[String]]))
    val rowlvlValidationCols = shapeConfig.columnValidations
      .filter(y => y.validationExpr.isInstanceOf[Some[String]])

    val partitionSpec = df
      .persist()
      .select(col("PARTITION_SPEC"))
      .filter(col("PARTITION_SPEC").isNotNull)
      .distinct()
      .collect()
      .map(_(0).toString())
      .toList
    val partitionValue = partitionSpec match {
      case x if x.intersect(List("yyyymm", "qtr")).length > 0 =>
        if (x.mkString.equals("yyyymm")) shapeMonitorConfig.yyyymm
        else shapeMonitorConfig.qtr
      case x if x.nonEmpty & shapeMonitorConfig.partitionValue.isDefined =>
        shapeMonitorConfig.partitionValue.get
      case _ => null
    }
    //println(partitionValue)

    val tableCountValidationDf = {
      val validationStatus = (partitionSpec, df) match {
        case (t, d) if (t.isEmpty & d.head(1).nonEmpty) => "PASS"
        case (t, d)
            if (t.nonEmpty & d
              .filter(s"PARTITION_VALUE = '${partitionValue}'")
              .head(1)
              .nonEmpty) =>
          "PASS"
        case _ => "FAIL"
      }
      val validationRule =
        s"Number of Records should be > 0 for latest partition/extraction"
      getTableLevelDefaultValidationDf(
        shapeConfig.dbName,
        shapeConfig.tableName,
        partitionSpec,
        partitionValue,
        validationRule,
        validationStatus)
    }

    val shapeChangesValidationDf =
      shapeChangesValidation(df, shapeConfig, partitionSpec, partitionValue)

    val seasonalityValidationDf = {
      val seasonalityCheckColumns = shapeConfig.columnValidations
        .filter(_.deseasonDetrendMetrics.nonEmpty)
        .map(_.columnName)
      if (seasonalityCheckColumns.nonEmpty) {
        Some(
          seasonalityCheck(
            df.filter($"COLUMN_NAME".isin(seasonalityCheckColumns: _*)),
            shapeConfig,
            partitionSpec,
            partitionValue))
      } else None
    }

    val uniqueColumnValidationDf = {
      if (shapeConfig.uniqueColumns.nonEmpty & df
          .filter(col("VALIDATIONS_COMPUTED") === "No")
          .head(1)
          .nonEmpty) {
        val validationRule =
          s"Unique Constraint Check on specified Column(s) : ${shapeConfig.uniqueColumns.mkString(",")}."
        val columnsList =
          df.select(col("COLUMN_NAME")).distinct().collect().toList.map(_(0).toString())
        if (columnsList
            .intersect(shapeConfig.uniqueColumns)
            .length == shapeConfig.uniqueColumns.length) {
          val partitionsList =
            df.filter(col("VALIDATIONS_COMPUTED") === "No")
              .select(
                coalesce(col("PARTITION_SPEC"), lit("null")),
                coalesce(col("PARTITION_VALUE"), lit("null")),
                coalesce(col("SUBPARTITION_SPEC"), lit("null")),
                coalesce(col("SUBPARTITION_VALUE"), lit("null")))
              .distinct()
              .collect()
              .toList
              .map(x =>
                (x(0).toString(), x(1).toString(), x(2).toString(), x(3).toString()))
          val partitionFiltersList = partitionsList.map(x => {
            (
              x,
              s" ${x._1}='${x._2}' and ${x._3}='${x._4}' and 1 = 1 "
                .replaceAll(s"null='null' and", s""))
          })

          val uniqueConstraintCheckDfList = {
            partitionFiltersList.map(x => {
              val sourceDf = sparkSession.sql(
                s"SELECT * FROM ${shapeConfig.dbName}.${shapeConfig.tableName}")
              val monthNormalizedSourceDf = monthColumns
                .diff(List("yyyymm"))
                .intersect(sourceDf.columns)
                .foldLeft(sourceDf)({ (df, column) =>
                  df.withColumn("yyyymm", col(column)).drop(col(column))
                })
              val qtrNormalizedSourceDf = qtrColumns
                .diff(List("qtr"))
                .intersect(sourceDf.columns)
                .foldLeft(monthNormalizedSourceDf)({ (df, column) =>
                  df.withColumn("qtr", col(column)).drop(col(column))
                })
              val validationStatus =
                if (qtrNormalizedSourceDf
                    .filter(s"${x._2}")
                    .groupBy(shapeConfig.uniqueColumns.map(col): _*)
                    .count()
                    .where("count > 1")
                    .head(1)
                    .isEmpty) "PASS"
                else "FAIL"
              Seq((shapeConfig.dbName, shapeConfig.tableName))
                .toDF("DATABASE_NAME", "TABLE_NAME")
                .withColumn("COLUMN_NAME", lit(null))
                .withColumn("COLUMN_TYPE", lit(null))
                .withColumn("COLUMN_SPEC", lit(null))
                .withColumn("COLUMN_DATATYPE", lit(null))
                .withColumn(
                  "PARTITION_SPEC",
                  lit(if (x._1._1 == "null") null else x._1._1))
                .withColumn(
                  "PARTITION_VALUE",
                  lit(if (x._1._2 == "null") null else x._1._2))
                .withColumn(
                  "SUBPARTITION_SPEC",
                  lit(if (x._1._3 == "null") null else x._1._3))
                .withColumn(
                  "SUBPARTITION_VALUE",
                  lit(if (x._1._4 == "null") null else x._1._4))
                .withColumn("VALIDATION_RULE", lit(validationRule))
                .withColumn("VALIDATION_STATUS", lit(validationStatus))
                .withColumn("NUM_INVALIDS", lit(0))
            })
          }
          Some(uniqueConstraintCheckDfList.reduce(_ union _))
        } else
          Some(
            getTableLevelDefaultValidationDf(
              shapeConfig.dbName,
              shapeConfig.tableName,
              List(),
              null,
              validationRule,
              "FAIL"))
      } else None
    }

    val statsDf = df.filter(col("VALIDATIONS_COMPUTED") === "No")
    val filteredDfForAggValidations = statsDf.filter(
      col("COLUMN_NAME").isin(aggValidationCols.map(y => y.columnName): _*))
    val defaultValidationsDf = statsDf
      .withColumn(
        "VALIDATION_RULE",
        lit("case when NUM_RECORDS = NUM_MISSING then 'WARN' else 'PASS' end"))
      .withColumn(
        "VALIDATION_STATUS",
        expr("case when NUM_RECORDS = NUM_MISSING then 'WARN' else 'PASS' end"))
    val rowLevelValidationsDf = rowlvlValidationCols
      .map(
        x =>
          statsDf
            .filter(s"COLUMN_NAME='${x.columnName}'")
            .withColumn(
              "VALIDATION_RULE",
              lit(x.validationExpr.get.filter(_ >= ' ').replace("  ", "")))
            .withColumn("VALIDATION_STATUS", lit(null)))
      .reduceOption(_ union _)
    val aggLevelValidationsDFList = aggValidationCols.map(
      column =>
        filteredDfForAggValidations
          .filter(s"COLUMN_NAME = '${column.columnName}'")
          .withColumn(
            "VALIDATION_RULE",
            column.maxNullPctg match {
              case Some(exp) => lit(exp)
              case None      => lit(null)
            })
          .withColumn(
            "VALIDATION_STATUS",
            column.maxNullPctg match {
              case Some(exp) => expr(exp)
              case None      => lit(null)
            })
          .union(
            filteredDfForAggValidations
              .filter(s"COLUMN_NAME = '${column.columnName}'")
              .withColumn(
                "VALIDATION_RULE",
                column.maxCardinality match {
                  case Some(exp) => lit(exp)
                  case None      => lit(null)
                })
              .withColumn(
                "VALIDATION_STATUS",
                column.maxCardinality match {
                  case Some(exp) => expr(exp)
                  case None      => lit(null)
                }))
          .union(
            filteredDfForAggValidations
              .filter(s"COLUMN_NAME = '${column.columnName}'")
              .withColumn(
                "VALIDATION_RULE",
                column.minCardinality match {
                  case Some(exp) => lit(exp)
                  case None      => lit(null)
                })
              .withColumn(
                "VALIDATION_STATUS",
                column.minCardinality match {
                  case Some(exp) => expr(exp)
                  case None      => lit(null)
                })))

    val validationDf = rowLevelValidationsDf match {
      case Some(x) =>
        aggLevelValidationsDFList.foldLeft(defaultValidationsDf)(_ union _).union(x)
      case None => aggLevelValidationsDFList.foldLeft(defaultValidationsDf)(_ union _)
    }

    val validationData = validationDf
      .filter("VALIDATION_RULE is not null")
      .withColumn(
        "NUM_INVALIDS",
        expr("case when VALIDATION_STATUS is null then NUM_INVALID else 0 end"))
      .select(
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
        col("VALIDATION_RULE"),
        col("VALIDATION_STATUS"),
        col("NUM_INVALIDS"))
      .union(tableCountValidationDf)
      .union(shapeChangesValidationDf)

    val validationDataWithSeasonalityCheck = seasonalityValidationDf match {
      case Some(x) => validationData.union(x)
      case None    => validationData
    }

    val validationFinalData = uniqueColumnValidationDf match {
      case Some(x) => validationDataWithSeasonalityCheck.union(x)
      case None    => validationDataWithSeasonalityCheck
    }

    df.unpersist()

    validationFinalData
      .withColumn(
        "REC_CRT_DT",
        lit(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date)))
      .withColumn("APPLICATION_ID", lit(sparkSession.sparkContext.applicationId))
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
        col("VALIDATION_RULE"),
        col("VALIDATION_STATUS"),
        col("NUM_INVALIDS"),
        col("REC_CRT_DT"))
  }

  def shapeChangesValidation(
      df: DataFrame,
      shapeConfig: ShapeConfiguration,
      timePartitionSpec: List[String],
      timePartitionValue: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val validationRule =
      s"Validating Shape Changes for current partition. WARN if not enough data available. NA if not applicable/already validated."
    val shapeChangeValidationDf = {
      if ((timePartitionSpec.nonEmpty & timePartitionValue == null) |
          (timePartitionSpec.nonEmpty & df
            .filter(col("PARTITION_VALUE") === timePartitionValue)
            .head(1)
            .isEmpty) |
          (timePartitionSpec.nonEmpty & df
            .filter(col("PARTITION_VALUE") <= timePartitionValue)
            .select(col("PARTITION_VALUE"))
            .distinct()
            .count() < 8)) {
        getTableLevelDefaultValidationDf(
          shapeConfig.dbName,
          shapeConfig.tableName,
          timePartitionSpec,
          timePartitionValue,
          validationRule,
          "WARN")
      } else if ((timePartitionSpec.isEmpty) |
          (timePartitionSpec.nonEmpty & df
            .filter(col("PARTITION_VALUE") === timePartitionValue and col(
              "VALIDATIONS_COMPUTED") === "No")
            .head(1)
            .isEmpty)) {
        getTableLevelDefaultValidationDf(
          shapeConfig.dbName,
          shapeConfig.tableName,
          timePartitionSpec,
          timePartitionValue,
          validationRule,
          "NA")
      } else {
        val tableLevelChangesValidationDf = df
          .filter(col("PARTITION_VALUE") <= timePartitionValue)
          .select(
            $"DATABASE_NAME",
            $"TABLE_NAME",
            $"PARTITION_SPEC",
            $"PARTITION_VALUE",
            $"SUBPARTITION_SPEC",
            $"SUBPARTITION_VALUE",
            $"NUM_RECORDS",
            $"RANK")
          .distinct()
          .withColumn("MIN_RANK", min(col("RANK")).over())
          .filter(col("RANK") < col("MIN_RANK") + 8)
          .withColumn(
            "PREV_NUM_RECORDS",
            lag("NUM_RECORDS", 1).over(Window.orderBy($"RANK".desc)))
          .filter(col("PREV_NUM_RECORDS").isNotNull)
          .withColumn(
            "DIFF_IN_NUM_RECORDS",
            (col("NUM_RECORDS") - col("PREV_NUM_RECORDS")).cast(DoubleType))
          .withColumn(
            "DIFF_IN_NUM_RECORDS_LIST",
            collect_list(col("DIFF_IN_NUM_RECORDS")).over(Window.orderBy($"RANK".desc)))
          .filter(col("RANK") === col("MIN_RANK"))
          .withColumn(
            "Q_EFF_VALUE",
            dixonsQValueUDF.getQValue(col("DIFF_IN_NUM_RECORDS_LIST")))
          .withColumn("COLUMN_NAME", lit(null))
          .withColumn("COLUMN_TYPE", lit(null))
          .withColumn("COLUMN_SPEC", lit(null))
          .withColumn("COLUMN_DATATYPE", lit(null))
          .withColumn("NUM_INVALIDS", lit(0))
          .withColumn(
            "VALIDATION_RULE_DESC",
            concat(
              lit("WHEN "),
              col("Q_EFF_VALUE"),
              lit(" <= 0.507 THEN 'PASS' WHEN "),
              col("Q_EFF_VALUE"),
              lit(" <= 0.568 THEN 'WARN' ELSE 'FAIL'.")))
          .withColumn(
            "VALIDATION_RULE",
            concat(
              lit(
                s"Validating Trend of NUM_RECORDS of the Table for Current Partition. "),
              col("VALIDATION_RULE_DESC")))
          .withColumn(
            "VALIDATION_STATUS", {
              expr(s"""CASE WHEN Q_EFF_VALUE <= 0.507 THEN 'PASS' 
                          WHEN Q_EFF_VALUE <= 0.568 THEN 'WARN'
                          ELSE 'FAIL' END""")
            })
          .select(
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
            col("VALIDATION_RULE"),
            col("VALIDATION_STATUS"),
            col("NUM_INVALIDS"))

        val columns_list =
          df.filter(col("PARTITION_VALUE") <= timePartitionValue)
            .withColumn("MIN_RANK", min(col("RANK")).over())
            .filter(col("RANK") === col("MIN_RANK") + 7)
            .withColumn(
              "SHAPES_LIST",
              when(
                col("COLUMN_DATATYPE") === "DoubleType",
                typedLit(List("NUM_MISSING", "MEAN", "MEDIAN"))).otherwise(
                typedLit(List("NUM_MISSING"))))
            .select(col("COLUMN_NAME"), explode(col("SHAPES_LIST")).alias("SHAPE_NAME"))
            .collect()
            .toList
            .map(x => (x(0).toString(), x(1).toString()))

        val columnLevelChangesValidationDf = {
          columns_list
            .map(x => {
              df.filter(col("PARTITION_VALUE") <= timePartitionValue and col(
                  "COLUMN_NAME") === x._1)
                .withColumn("SHAPE_VALUES", col(x._2))
                .select(
                  $"DATABASE_NAME",
                  $"TABLE_NAME",
                  $"PARTITION_SPEC",
                  $"PARTITION_VALUE",
                  $"SUBPARTITION_SPEC",
                  $"SUBPARTITION_VALUE",
                  $"SHAPE_VALUES",
                  $"COLUMN_NAME",
                  $"COLUMN_TYPE",
                  $"COLUMN_SPEC",
                  $"COLUMN_DATATYPE",
                  $"RANK")
                .distinct()
                .withColumn("MIN_RANK", min(col("RANK")).over())
                .filter(col("RANK") < col("MIN_RANK") + 8)
                .withColumn(
                  "PREV_SHAPE_VALUES",
                  lag("SHAPE_VALUES", 1).over(Window.orderBy($"RANK".desc)))
                .filter(col("PREV_SHAPE_VALUES").isNotNull)
                .withColumn(
                  "DIFF_IN_SHAPE_VALUES",
                  (col("SHAPE_VALUES") - col("PREV_SHAPE_VALUES")).cast(DoubleType))
                .withColumn(
                  "DIFF_IN_SHAPE_VALUES_LIST",
                  collect_list(col("DIFF_IN_SHAPE_VALUES")).over(
                    Window.orderBy($"RANK".desc)))
                .filter(col("RANK") === col("MIN_RANK"))
                .withColumn(
                  "Q_EFF_VALUE",
                  dixonsQValueUDF.getQValue(col("DIFF_IN_SHAPE_VALUES_LIST")))
                .withColumn("NUM_INVALIDS", lit(0))
                .withColumn(
                  "VALIDATION_RULE_DESC",
                  concat(
                    lit("WHEN "),
                    col("Q_EFF_VALUE"),
                    lit(" <= 0.507 THEN 'PASS' WHEN "),
                    col("Q_EFF_VALUE"),
                    lit(" <= 0.568 THEN 'WARN' ELSE 'FAIL'.")))
                .withColumn(
                  "VALIDATION_RULE",
                  concat(
                    lit(s"Validating Trend for ${x._1} Column on ${x._2} Metric for Current Partition. "),
                    col("VALIDATION_RULE_DESC")))
                .withColumn(
                  "VALIDATION_STATUS", {
                    expr(s"""CASE WHEN Q_EFF_VALUE <= 0.507 THEN 'PASS' 
                              WHEN Q_EFF_VALUE <= 0.568 THEN 'WARN'
                              ELSE 'FAIL' END""")
                  })
                .select(
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
                  col("VALIDATION_RULE"),
                  col("VALIDATION_STATUS"),
                  col("NUM_INVALIDS"))
            })
            .reduce(_ union _)
        }

        tableLevelChangesValidationDf.union(columnLevelChangesValidationDf)
      }
    }

    shapeChangeValidationDf
  }

  def seasonalityCheck(
      df: DataFrame,
      shapeConfig: ShapeConfiguration,
      timePartitionSpec: List[String],
      timePartitionValue: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val validationRule =
      s"Validating Seasonality for current partition. WARN if not enough data Partitions available. ERROR if Missig Data Partitions. NA if not applicable/already validated."
    val seasonalityValidationDf = {
      if ((timePartitionSpec.nonEmpty & timePartitionValue == null) |
          (timePartitionSpec.nonEmpty & df
            .filter(col("PARTITION_VALUE") <= timePartitionValue)
            .select(col("PARTITION_VALUE"))
            .distinct()
            .count() <= 37)) {
        println("Not enough partitions avaliable to perform deseasoning")
        getTableLevelDefaultValidationDf(
          shapeConfig.dbName,
          shapeConfig.tableName,
          timePartitionSpec,
          timePartitionValue,
          validationRule,
          "WARN")
      } else if ((timePartitionSpec.isEmpty) |
          (timePartitionSpec.nonEmpty & df
            .filter(col("PARTITION_VALUE") === timePartitionValue and col(
              "VALIDATIONS_COMPUTED") === "No")
            .head(1)
            .isEmpty)) {
        //  println("No uncomputed partition validation")
        getTableLevelDefaultValidationDf(
          shapeConfig.dbName,
          shapeConfig.tableName,
          timePartitionSpec,
          timePartitionValue,
          validationRule,
          "NA")
      } else if ((timePartitionSpec.nonEmpty & df
          .filter(col("PARTITION_VALUE") === timePartitionValue)
          .head(1)
          .isEmpty) |
          //TODO: Identifying Missing data partitions in case of other period types.
          (timePartitionSpec(0) == "yyyymm" &
            df.filter(col("PARTITION_VALUE") <= timePartitionValue and col(
                "PARTITION_VALUE") >= timePartitionValue.toInt - 300)
              .select(col("PARTITION_VALUE"))
              .distinct()
              .count() <= 36)) {
        //  println("missing partition")
        getTableLevelDefaultValidationDf(
          shapeConfig.dbName,
          shapeConfig.tableName,
          timePartitionSpec,
          timePartitionValue,
          validationRule,
          "ERROR")
      } else {
        //  println("Actual deseason flow final ::::")
        val deseasonMetricsList =
          sparkSession
            .createDataFrame(
              shapeConfig.columnValidations.filter(_.deseasonDetrendMetrics.nonEmpty))
            .select(
              col("columnName"),
              explode(col("deseasonDetrendMetrics")).alias("deseasonMetrics"))
            .map(
              x =>
                (
                  x.getString(0),
                  x.getStruct(1).getString(0),
                  if (x.getStruct(1).get(1) == null) List()
                  else List(x.getStruct(1).getDouble(1), x.getStruct(1).getDouble(2))))
            .collect
            .toList

        val validationResultedDf = deseasonMetricsList
          .map(x => {
            //print(x)
            df.filter(col("PARTITION_VALUE") <= timePartitionValue)
              .withColumn("MIN_RANK", min(col("RANK")).over())
              .filter(col("COLUMN_NAME") === x._1)
              .withColumn("CURR_METRIC_VALUE", col(x._2))
              .withColumn(
                "SEASONED_METRIC_VALUE",
                lag("CURR_METRIC_VALUE", 12, 0).over(Window.orderBy($"RANK".desc)))
              .filter(col("SEASONED_METRIC_VALUE").isNotNull)
              .withColumn(
                "DESEASONED_METRIC_VALUE",
                col("CURR_METRIC_VALUE") / col("SEASONED_METRIC_VALUE"))
              .withColumn(
                "PREV_DESEASONED_METRIC_VALUE",
                lag("DESEASONED_METRIC_VALUE", 1, 0).over(Window.orderBy($"RANK".desc)))
              .filter(col("PREV_DESEASONED_METRIC_VALUE").isNotNull)
              .withColumn(
                "DIFF_DESEASONED_METRIC_VALUE",
                col("DESEASONED_METRIC_VALUE") - col("PREV_DESEASONED_METRIC_VALUE"))
              .withColumn(
                "MEAN_DIFF_DESEASONED_METRIC_VALUE",
                avg("DIFF_DESEASONED_METRIC_VALUE").over(
                  Window.orderBy(col("RANK").asc).rowsBetween(1, 50)))
              .withColumn(
                "STDDEV_DIFF_DESEASONED_METRIC_VALUE",
                stddev("DIFF_DESEASONED_METRIC_VALUE").over(
                  Window.orderBy(col("RANK").asc).rowsBetween(1, 50)))
              .filter(col("RANK") === col("MIN_RANK"))
              .withColumn(
                "VALIDATION_RULE_DESC", {
                  if (x._3.isEmpty) {
                    concat(
                      lit("WHEN "),
                      col("DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" >= ("),
                      col("MEAN_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" - 2 * "),
                      col("STDDEV_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(") and "),
                      col("DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" <= ("),
                      col("MEAN_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" + 2 * "),
                      col("STDDEV_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(") THEN 'PASS' "),
                      lit("WHEN "),
                      col("DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" >= ("),
                      col("MEAN_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" - 3 * "),
                      col("STDDEV_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(") and "),
                      col("DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" <= ("),
                      col("MEAN_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" + 3 * "),
                      col("STDDEV_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(") THEN 'WARN' "),
                      lit("ELSE 'FAIL'."))
                  } else {
                    //println("custom sigma formation description ::: ")
                    concat(
                      lit("WHEN "),
                      col("DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" >= ("),
                      col("MEAN_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(s" - ${-x._3(0)} * "),
                      col("STDDEV_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(") and "),
                      col("DIFF_DESEASONED_METRIC_VALUE"),
                      lit(" <= ("),
                      col("MEAN_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(s" + ${x._3(1)} * "),
                      col("STDDEV_DIFF_DESEASONED_METRIC_VALUE"),
                      lit(") THEN 'PASS' ELSE 'FAIL'."))
                  }
                })
              .withColumn(
                "VALIDATION_RULE",
                concat(
                  lit(s"Validating Sesonality check for ${x._1} Column on ${x._2} Metric for Current Partition. "),
                  col("VALIDATION_RULE_DESC")))
              .withColumn(
                "VALIDATION_STATUS", {
                  if (x._3.isEmpty) {
                    expr(s"""CASE WHEN DIFF_DESEASONED_METRIC_VALUE >= (MEAN_DIFF_DESEASONED_METRIC_VALUE - 2 * STDDEV_DIFF_DESEASONED_METRIC_VALUE) and DIFF_DESEASONED_METRIC_VALUE <= (MEAN_DIFF_DESEASONED_METRIC_VALUE + 2 * STDDEV_DIFF_DESEASONED_METRIC_VALUE) THEN 'PASS'
                            WHEN DIFF_DESEASONED_METRIC_VALUE >= (MEAN_DIFF_DESEASONED_METRIC_VALUE - 3 * STDDEV_DIFF_DESEASONED_METRIC_VALUE) and DIFF_DESEASONED_METRIC_VALUE <= (MEAN_DIFF_DESEASONED_METRIC_VALUE + 3 * STDDEV_DIFF_DESEASONED_METRIC_VALUE) THEN 'WARN'
                            ELSE 'FAIL' END""")
                  } else {
                    //println("custom sigma formation :::")
                    expr(
                      s"CASE WHEN DIFF_DESEASONED_METRIC_VALUE >= (MEAN_DIFF_DESEASONED_METRIC_VALUE - ${-x._3(0)} * STDDEV_DIFF_DESEASONED_METRIC_VALUE) and DIFF_DESEASONED_METRIC_VALUE <= (MEAN_DIFF_DESEASONED_METRIC_VALUE + ${x._3(1)} * STDDEV_DIFF_DESEASONED_METRIC_VALUE)  THEN 'PASS' ELSE 'FAIL' END")
                  }
                })
              .withColumn("NUM_INVALIDS", lit(0))
              /*       .select(
            col("DATABASE_NAME"),
            col("TABLE_NAME"),
            col("COLUMN_NAME"),
            col("PARTITION_VALUE"),
            col("RANK"),
            col("CURR_METRIC_VALUE"),
            col("SEASONED_METRIC_VALUE"),
            col("DESEASONED_METRIC_VALUE"),
            col("PREV_DESEASONED_METRIC_VALUE"),
            col("DIFF_DESEASONED_METRIC_VALUE"),
            col("MEAN_DIFF_DESEASONED_METRIC_VALUE"),
            col("STDDEV_DIFF_DESEASONED_METRIC_VALUE")
            )*/
              .select(
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
                col("VALIDATION_RULE"),
                col("VALIDATION_STATUS"),
                col("NUM_INVALIDS"))
          })
          .reduce(_ union _)

        validationResultedDf
      }
    }
    //seasonalityValidationDf.show(100,false)
    seasonalityValidationDf
  }

  def getTableLevelDefaultValidationDf(
      databaseName: String,
      tableName: String,
      timePartitionSpec: List[String],
      timePartitionValue: String,
      validationRule: String,
      validationStatus: String)(implicit sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._
    Seq((databaseName, tableName))
      .toDF("DATABASE_NAME", "TABLE_NAME")
      .withColumn("COLUMN_NAME", lit(null))
      .withColumn("COLUMN_TYPE", lit(null))
      .withColumn("COLUMN_SPEC", lit(null))
      .withColumn("COLUMN_DATATYPE", lit(null))
      .withColumn(
        "PARTITION_SPEC",
        lit(if (!timePartitionSpec.isEmpty) timePartitionSpec.mkString else null))
      .withColumn("PARTITION_VALUE", lit(timePartitionValue))
      .withColumn("SUBPARTITION_SPEC", lit(null))
      .withColumn("SUBPARTITION_VALUE", lit(null))
      .withColumn("VALIDATION_RULE", lit(validationRule))
      .withColumn("VALIDATION_STATUS", lit(validationStatus))
      .withColumn("NUM_INVALIDS", lit(0))
  }

}
