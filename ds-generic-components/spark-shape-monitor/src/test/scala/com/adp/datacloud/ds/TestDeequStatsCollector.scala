package com.adp.datacloud.ds

import java.io.{PrintWriter, StringWriter}
import java.time.LocalTime

import com.adp.datacloud.cli.{ShapeMonitorConfig, ShapeMonitorOptions}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

class TestDeequStatsCollector {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    val starttime = LocalTime.now()
    val args: Array[String] =
      "--xml-config-file src/test/resources/ev4_shape_monitor_config.xml --db-jdbc-url cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1 --db-username ADPI_DXR --db-password adpi --partition-value 201901"
        .split(" ")

    val ShapeMonitorConfig = ShapeMonitorOptions.parse(args)

    implicit val sparkSession = SparkSession
      .builder()
      .appName(ShapeMonitorConfig.applicationName)
      .config("spark.sql.warehouse.dir", "/Users/somapava/Documents/ADP_Work/hive")
      .config("spark.driver.maxResultSize", "2g")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

//    val virtualcolumns : List[(String,String)] =List(("annual_base_band_filtered", "case when annual_base_band_ <> 'Unknown' then  annual_base_band_ end"),("crc32_ssn","crc32(ssn)"))
    val virtualcolumns: List[(String, String)] = List((
      "virtual_complex_column",
      """case when source_hr is not null then map(source_hr,source_hr) else null end"""))
    val annual_base_val = new ShapeValidation(
      "annual_base_",
      Some("""case when 1=1 
                                              and annual_base_ is not null 
                                              and annual_base_ < 400000
                                              then 0 else 1 end"""))
    val rate_type_val = new ShapeValidation(
      "rate_type_",
      maxCardinality = Some(
        "case when NUM_APPROX_UNIQUE_VALUES > 2 then 'FAIL' else 'PASS' end"),
      maxNullPctg = Some(
        "case when NUM_MISSING / NUM_RECORDS > 30/100 then 'FAIL' else 'PASS' end"))
    val work_state_val = new ShapeValidation(
      "work_state_",
      Some("""case when 1=1
                                                and work_state_ in ('CA','GA','WA','NY')
                                                then 0 else 1 end"""),
      maxNullPctg = Some(
        "case when NUM_MISSING / NUM_RECORDS > 10/100 then 'FAIL' else 'PASS' end"))

    val shapeConfiguration = ShapeConfiguration(
      "distdsbase",
      "employee_monthly",
      List(),
      List(),
      List(),
      virtualcolumns,
      columnValidations = List(annual_base_val, rate_type_val))
    println(shapeConfiguration)
    val finalDf =
      sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")

    val df1 = virtualcolumns
      .foldLeft(finalDf)((finalDf, virtualcolumns) =>
        finalDf.withColumn(virtualcolumns._1, expr(virtualcolumns._2)))
      .select(
        "employee_guid",
        "work_state_",
        "annual_base_",
        "rate_type_",
        "annual_base_band_",
        "virtual_complex_column",
        "source_hr",
        "yyyymm"
      ) //"crc32_ssn","annual_base_band_filtered"
    val transformedInputDf = shapeConfiguration.columnValidations
      .foldLeft(df1)({ (df, column) =>
        column.validationExpr match {
          case Some(x) => df.withColumn(s"${column.columnName}_SHAPE_VALDN", expr(x))
          case None    => df
        }
      })

    //  .select("employee_guid","source_pr","source_hr","ooid","ssn","ap_region_code","annual_base_band_","regular_temporary_","hire_date_","annual_base_","gross_wages_","yyyymm")
    //  .filter(col("source_pr").isNotNull)

    val partitionCol = Option(List("yyyymm"))

    transformedInputDf.printSchema
    val statCollector =
      new DeequStatsCollector(transformedInputDf, shapeConfiguration, partitionCol)
    val statsCollectorDf = statCollector.collect()
    /*   for(statsDf<-statsCollectorDf) {
      statsCollectorDf.show(1000,false)


    }*/

    // sparkSession.read.parquet("src/test/resources/column_statistics").show(1000,false)
    //statsCollectorDf.show(500, false)
    //saveLocalStats(statsCollectorDf)

    /*  val validation= new ShapeValidations

    val validationDf = validation.validateShape(statsCollectorDf, shapeConfiguration)

    validationDf.show(500,false)
    validationDf.printSchema*/

    //val cleanedDf = statsCollectorDf.columns.foldLeft(statsCollectorDf)((df, column) => df.withColumn(column, when(col(column) === (lit("NaN")), lit(null)).otherwise(col(column)) )   )
    //cleanedDf.show(50, false)
    //saveStatistics(statsCollectorDf, ShapeMonitorConfig)
    statsCollectorDf.show(1000, false)
    val endtime = LocalTime.now()
    println(s"Started at ${starttime} ::::: Complted at ${endtime}")
    Thread.sleep(10000)

  }

  def saveLocalStats(stats: DataFrame) = {

    stats
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save("src/test/resources/column_statistics")
  }
  // Write collected Column statistics to rdbms table
  def saveStatistics(
      statisticsDataFrame: DataFrame,
      ShapeMonitorConfig: ShapeMonitorConfig) {
    try {
      statisticsDataFrame.write
        .format("jdbc")
        .options(ShapeMonitorConfig.jdbcProperties.asScala.toMap)
        .option("dbtable", "COLUMN_STATISTICS")
        .mode(SaveMode.Append)
        .save()

      logger.info("Statistics are successfully written to TABLE_STATISTICS table")
    } catch {
      case e: Exception =>
        logger.error(s"Failed while writing statistics to rdbms table: ", e)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
    }
  }
}
