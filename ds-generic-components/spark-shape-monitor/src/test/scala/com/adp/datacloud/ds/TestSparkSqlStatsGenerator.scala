package com.adp.datacloud.ds

import java.io.{PrintWriter, StringWriter}

import com.adp.datacloud.cli.{ShapeMonitorConfig, ShapeMonitorOptions}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

class TestSparkSqlStatsGenerator {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) = {

    val args: Array[String] =
      "--xml-config-file src/test/resources/ev4_shape_monitor_config.xml --db-jdbc-url cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1 --db-username ADPI_DXR --db-password adpi"
        .split(" ")

    val ShapeMonitorConfig = ShapeMonitorOptions.parse(args)

    implicit val sparkSession = SparkSession.builder
      .appName("Spark SQL Stats Generator")
      .master("local")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    val partitioncolumns: List[String] = List("yyyymm")
    val dbname                         = "default"
    val tablename                      = "employee_monthly"
    /*val virtualcolumns : List[(String,String)] = List(("crc32_employee_guid","crc32(employee_guid)"),
                                                                    ("crc32_work_state_","crc32(work_state_)"))*/
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
      maxCardinality = Some("case when NUM_APPROX_UNIQUE_VALUES > 2 then 1 else 0 end"),
      maxNullPctg = Some(
        "case when NUM_MISSING / NUM_RECORDS > 30/100 then 1 else 0 end"))
    val work_state_val = new ShapeValidation(
      "work_state_",
      Some("""case when 1=1
                                                and work_state_ in ('CA','GA','WA','NY')
                                                then 0 else 1 end"""),
      maxNullPctg = Some(
        "case when NUM_MISSING / NUM_RECORDS > 10/100 then 1 else 0 end"))

    val tableConfig = ShapeConfiguration(
      dbName            = dbname,
      tableName         = tablename,
      partitionColumns  = partitioncolumns,
      virtualColumns    = virtualcolumns,
      columnValidations = List(annual_base_val, rate_type_val, work_state_val))

    /* val sql = " select employee_guid,ooid,work_state_,annual_base_,rate_type_ ,crc32(employee_guid) as crc32_employee_guid,crc32(work_state_) as crc32_work_state_ , yyyymm, source_hr from employee_monthly where source_hr is not null"*/
    val df = sparkSession.read
      .parquet("src/test/resources/employee_monthly_shapetest_dataset")
      .filter("annual_base_ is null")
    val df1 = virtualcolumns
      .foldLeft(df)((df, virtualcolumns) =>
        df.withColumn(virtualcolumns._1, expr(virtualcolumns._2)))
      .select(
        "employee_guid",
        "work_state_",
        "annual_base_",
        "rate_type_",
        "annual_base_band_",
        "virtual_complex_column",
        "yyyymm",
        "source_hr"
      ) //"crc32_ssn","annual_base_band_filtered"

    val transformedInputDf = tableConfig.columnValidations
      .foldLeft(df1)({ (df, column) =>
        column.validationExpr match {
          case Some(x) => df.withColumn(s"${column.columnName}_SHAPE_VALDN", expr(x))
          case None    => df
        }
      })
    transformedInputDf.show()
    transformedInputDf.createTempView(tablename)

    val statsObject = new SparkSqlStatsCollector(
      inputDf          = transformedInputDf,
      shapeConfig      = tableConfig,
      partitioncolumns = partitioncolumns)

    /*val statsObject = new DeequStatsCollector(   df = df1,
                                                 shapeConfig = tableConfig,
                                                 partitioncolumns = Some(partitioncolumns))*/
    // val x = statsObject.generateGroupingSets()
    // x.foreach(print)

    val stats = statsObject.collect()
    stats.show(1000, false)
    //saveLocalStats(stats)

    /*  println(statsObject.collect().limit(20) .count())

    saveStatistics(statsObject.collect().limit(20), ShapeMonitorConfig)
     */
    // Thread.sleep(100000)
  }

  def saveLocalStats(stats: DataFrame) = {

    stats.write
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
