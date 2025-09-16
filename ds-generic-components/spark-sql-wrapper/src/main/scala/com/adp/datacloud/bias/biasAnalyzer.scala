package com.adp.datacloud.bias

import java.util.Calendar
import com.adp.datacloud.cli.{BiasAnalyzerConfig, biasAnalyzerOptions}
import com.adp.datacloud.ds.aws.SecretsStore
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

object biasAnalyzer {

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val biasAnalyzerConfig = biasAnalyzerOptions.parse(args)
    val tableName          = biasAnalyzerConfig.inputFilePath.split("/").last

    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(s"BIAS_ANALYSIS_$tableName")
      .enableHiveSupport()
      .getOrCreate()

    val filesString = biasAnalyzerConfig.files
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

    computeMetrics(biasAnalyzerConfig)
  }

  def getESCredentialsConfMap(config: BiasAnalyzerConfig)(implicit
      sparkSession: SparkSession): Map[String, String] = {

    val esUserName = if (config.esUsername.isDefined) {
      config.esUsername.get
    } else if (sparkSession.conf.getOption("spark.es.net.http.auth.user").isDefined) {
      sparkSession.conf.get("spark.es.net.http.auth.user")
    } else ""

    if (esUserName.nonEmpty) {
      val esPassword = config.esPwd match {
        case Some(y) => y
        case None =>
          logger.info(s"ES_CREDS: fetching credentials for esUserName $esUserName")
          SecretsStore.getCredential("__ORCHESTRATION_ES_CREDENTIALS__", esUserName).get
      }
      Map(
        "spark.es.net.http.auth.user" -> esUserName,
        "es.net.http.auth.user" -> esUserName,
        "spark.es.net.http.auth.pass" -> esPassword,
        "es.net.http.auth.pass" -> esPassword)
    } else {
      throw new Exception("Invalid Credentials Configuration")
    }
  }

  def computeMetrics(config: BiasAnalyzerConfig)(implicit
      sparkSession: SparkSession): Unit = {

    logger.info(s"INPUT_SQL: ${config.sql}")

    val inputDF = sparkSession.sql(config.sql)
    // Print the Shape of the DataFrame
    logger.info(s"Input Data ${dataFrameShape(inputDF)}")

    // Input Arguments
    val dimensionList = config.biasCheckColumns
    val groupColumns  = config.groupByColumns
    val frequency     = config.frequency
    val projectShortName = sparkSession.conf
      .get("spark.orchestration.package.short.name", "NA")
    val databaseName: String = config.inputParamsMap("__GREEN_MAIN_DB__")

    // Validate if the dimension exists in the data provided.
    dimensionList.foreach(
      dimension =>
        require(
          inputDF.columns.contains(dimension),
          s"ERROR: $dimension column is not present in the input data."))

    // Bypass if group not specified
    val groupList = {
      if (groupColumns.isEmpty) List("group") else groupColumns
    }
    val df = {
      if (groupColumns.isEmpty) inputDF.withColumn("group", lit("default"))
      else inputDF
    }

    // Compute metrics per dimension
    val computedMetricsArray = groupList.flatMap(group =>
      dimensionList.map(dimension => {
        val groupByCol: List[String] = List(group, dimension)

        // Compute Difference of Means and Residuals metric
        val diffOfMeansAndRes =
          computeDiffOfMeansAndResiduals(df, groupByCol, dimension, group)

        // Compute Equal Opportunity metric
        val equalOppDF = df.filter("y == 1")
        val equalOppMetric = computeDiffOfMeansMetric(
          equalOppDF,
          groupByCol,
          dimension,
          group,
          "equal_opportunity")

        // Compute Equal Mis-Opportunity metric
        val equalMisOppDF = df.filter("y != 1")
        val equalMisOppMetric = computeDiffOfMeansMetric(
          equalMisOppDF,
          groupByCol,
          dimension,
          group,
          "equal_misopportunity")

        // Merging in to a single dataframe
        val finalDF = diffOfMeansAndRes
          .join(equalOppMetric, groupByCol)
          .join(equalMisOppMetric, groupByCol)

        finalDF
          .withColumn("dimension", lit(s"$dimension"))
          .withColumn("group", lit(s"$group"))
          .withColumnRenamed(s"$dimension", "protected_class")
          .withColumnRenamed(s"$group", "group_value")

      }))

    val metricsDF = computedMetricsArray.reduce(_.union(_))

    val truncFormat = getTruncFormat(s"$frequency")
    val period_     = getPeriod(s"$frequency")

    // Unique Id computation --> Group + Group Value + Dimension + Protected Class + period_
    val idColumns: List[String] =
      List("group", "group_value", "dimension", "protected_class", "period_")

    val computedIndexName = s"bias_analysis_$projectShortName".toLowerCase
    val latestVersion =
      getLatestVersion(s"$databaseName", s"$computedIndexName", s"$period_")

    // Re-arranging columns
    val reorderedColumnNames: Array[String] = Array(
      "group",
      "group_value",
      "dimension",
      "protected_class",
      "target_mean",
      "pred_mean",
      "target_threshold",
      "pred_threshold",
      "diff_of_target_means",
      "diff_of_pred_means",
      "diff_of_resiudals",
      "equal_opportunity",
      "equal_misopportunity",
      "unique_id",
      "timestamp",
      "truncated_timestamp",
      "period_",
      "version")

    val computedMetricsDF = metricsDF
      .withColumn(
        "timestamp",
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
      .withColumn(
        "truncated_timestamp",
        date_format(
          date_trunc(s"$truncFormat", current_timestamp()),
          "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
      .withColumn("period_", lit(s"$period_"))
      .withColumn("version", lit(s"$period_$latestVersion"))
      .withColumn("unique_id", concat(idColumns.map(c => col(c)): _*))
      .select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

//    logger.info(computedMetricsDF.show(false))

    // Write to Hive
    val hiveWriter = HiveDataFrameWriter("parquet", partitionSpec = None)
    hiveWriter.insertSafely(
      s"$databaseName.$computedIndexName",
      computedMetricsDF,
      isOverWrite = false)

    val esConfigurationMapping =
      getESCredentialsConfMap(config).+("es.mapping.id" -> "unique_id")
    // Write to Index
    computedMetricsDF.saveToEs(s"$computedIndexName", esConfigurationMapping)

  }

  /**
   * Computes the latest version
   */
  def getLatestVersion(databaseName: String, table: String, period: String)(implicit
      sparkSession: SparkSession): String = {
    val tableExists =
      sparkSession.catalog.tableExists(s"$databaseName", s"$table")
    if (!tableExists) "v1"
    else {
      val maxVersion = sparkSession
        .sql(s"Select max(version) From $databaseName.$table where period_== '$period'")
        .collect()(0)
        .getString(0)
      val newVersion: Int = {
        if (maxVersion == null) 1
        else maxVersion.split("v")(1).toInt + 1
      }
      s"v$newVersion"
    }
  }

  /**
   * Gets the truncformat value
   */
  def getTruncFormat(fmt: String): String =
    fmt match {
      case "M" => "MONTH"
      case "Q" => "QUARTER"
      case "Y" => "YEAR"
      case "W" => "WEEK"
      case _   => "MONTH" // the default, catch-all
    }

  /**
   * Gets the formatted date
   */
  def getPeriod(fmt: String): String = {
    val calDate        = Calendar.getInstance()
    val currentYear    = calDate.get(Calendar.YEAR)
    val currentMonth   = calDate.get(Calendar.MONTH)
    val currentQuarter = (currentMonth / 3) + 1
    val currentWeek    = calDate.get(Calendar.WEEK_OF_YEAR)

    fmt match {
      case "M" => s"${currentYear}M$currentMonth"
      case "Q" => s"${currentYear}Q$currentQuarter"
      case "Y" => s"${currentYear}Q$currentQuarter"
      case "W" => s"${currentYear}W$currentWeek"
      case _   => s"${currentYear}M$currentMonth" // the default, catch-all
    }
  }

  /**
   * Computes the Difference Of Means and Residuals metric
   */
  def computeDiffOfMeansAndResiduals(
      df: DataFrame,
      groupByCol: List[String],
      dimension: String,
      group: String): DataFrame = {
    val newDF = df.withColumn("residual", col("y_hat") - col("y"))

    // Individual category avg. calculation
    val yMean        = avg("y").as("target_mean")
    val yHatMean     = avg("y_hat").as("pred_mean")
    val residualMean = avg("residual").as("residual_mean")
    val groupByCols  = groupByCol.map(c => col(s"$c"))

    val summaryDF = newDF
      .groupBy(groupByCols: _*)
      .agg(yMean, yHatMean, residualMean)
    //      .orderBy(groupByCols: _*)

    // Sans category avg. calculation
    val distinctVal =
      newDF.select(dimension).distinct().collect().flatMap(_.toSeq.toList)
    val ySansMean        = avg("y").as("target_sans_mean")
    val yHatSansMean     = avg("y_hat").as("pred_sans_mean")
    val residualSansMean = avg("residual").as("residual_sans_mean")

    val sansArray =
      distinctVal.map(row => {
        newDF
          .where(s"$dimension != '$row'")
          .groupBy(col(s"$group"))
          .agg(ySansMean, yHatSansMean, residualSansMean)
          .withColumn(s"$dimension", lit(s"$row"))
      })

    val sansDF = sansArray.reduce(_.union(_))

    // Merging dataframes(summaryDF and sansDF)
    val joinExpr = groupByCol
    val joinType = "left"
    val cols =
      Seq("residual_mean", "target_sans_mean", "pred_sans_mean", "residual_sans_mean")

    val combinedDF = summaryDF
      .join(sansDF, joinExpr, joinType)
      .withColumn("diff_of_target_means", col("target_mean") - col("target_sans_mean"))
      .withColumn("diff_of_pred_means", col("pred_mean") - col("pred_sans_mean"))
      .withColumn("diff_of_resiudals", col("residual_mean") - col("residual_sans_mean"))
      .drop(cols: _*)
    // Old: group,dimension,target_mean,pred_mean,residual_mean,target_sans_mean,pred_sans_mean,residual_sans_mean
    // New: group,dimension,target_mean,pred_mean,diff_of_target_means,diff_of_pred_means,diff_of_resiudals,

    val targetAverage = avg("y").as("target_threshold")
    val predAverage   = avg("y_hat").as("pred_threshold")

    val dfThresh = newDF
      .groupBy(col(s"$group"))
      .agg(targetAverage, predAverage)

    val joinExprs = List(group)
    val mergedDF  = combinedDF.join(dfThresh, joinExprs, "outer")

    mergedDF
  }

  /**
   * Computes the Difference Of Means metric for Equal Opportunity & Mis-Opportunity metric
   */
  def computeDiffOfMeansMetric(
      newDF: DataFrame,
      groupByCol: List[String],
      dimension: String,
      group: String,
      columnName: String): DataFrame = {
    val yHatMean    = avg("y_hat").as("pred_mean")
    val groupByCols = groupByCol.map(c => col(s"$c"))

    val summaryDF = newDF
      .groupBy(groupByCols: _*)
      .agg(yHatMean)
    //      .orderBy(groupByCols: _*)

    // Sans category avg. calculation
    val distinctVal =
      newDF.select(dimension).distinct().collect().flatMap(_.toSeq.toList)

    val sansArray =
      distinctVal.map(row => {
        newDF
          .where(s"$dimension != '$row'")
          .groupBy(col(s"$group"))
          .agg(avg("y_hat").as("pred_sans_mean"))
          .withColumn(s"$dimension", lit(s"$row"))

      })

    val sansDF = sansArray.reduce(_.union(_))

    // Merging dataframes(summaryDF and sansDF)
    val joinExpr = groupByCol
    val joinType = "left"
    val cols     = Seq("pred_mean", "pred_sans_mean")

    val mergedDF = summaryDF
      .join(sansDF, joinExpr, joinType)
      .withColumn(s"$columnName", col("pred_mean") - col("pred_sans_mean"))
      .drop(cols: _*)
    // Old: group,dimension,pred_mean,pred_sans_mean
    // New: group,dimension,$columnName
    mergedDF
  }

  /**
   * Computes the DataFrame Shape
   */
  def dataFrameShape(df: DataFrame): String = {
    s"Shape: (${df.count()}, ${df.columns.length})"
  }

}
