package com.adp.datacloud.ds

import com.adp.datacloud.cli.ShapeMonitorOptions
import org.apache.spark.sql.SparkSession

class TestProfiling {

  def main(args1: Array[String]) = {

    val args: Array[String] =
      "--xml-config-file employee_config.xml --dxr-jdbc-url cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1 --oracle-wallet-location /app/oraclewallet"
        .split(" ")

    val ShapeMonitorConfig = ShapeMonitorOptions.parse(args)

    implicit val sparkSession = SparkSession.builder
      .appName("Test Profiling on Sample")
      .master("local")
      .getOrCreate()
    val globalTimePartitions: List[String] = List("yyyymm", "ingest_month", "qtr")

    val df = sparkSession.read
      .parquet("src/test/resources/employee_monthly_shapetest_dataset")
      .filter("source_hr is not null")
    // partitions columns may or may not be present at the given table
    val collector        = new ShapeCollector
    val partitionColumns = List("yyyymm")
    val columns = Array(
      ("employee_guid", "string"),
      ("ooid", "string"),
      ("annual_base_", "double"),
      ("rate_type_", "string"))

    /*collector.setUsablePartitions(globalTimePartitions)

    val partitionCombos = collector.getPartitionCombos(df)

    //  1. create rollup for all partition values
    //  2. create filter combination for all partition values
    //  3. get all the existing partition combos

    val tableProfiles = collector.profileBuilder(df,columns.toList,partitionCombos)

    //tableProfiles.foreach(print)
    val statsDf = tableProfiles.map(x=> TableStatistics(
                                                       DATABASE_NAME= "livedsmaindc1",
                                                       TABLE_NAME = "Employee_monthly",
                                                       COLUMN_NAME = x.COLUMN_NAME,
                                                       COLUMN_TYPE = "PLAIN",
                                                       COLUMN_DATATYPE = x.COLUMN_TYPE,
                                                       PARTITION_SPEC = x.PARTITION match {
                                                                case Some(m) => Some(m.split('=').head.trim())
                                                                case None => None
                                                                },
                                                       PARTITION_VALUE = x.PARTITION match {
                                                                case Some(m) => Some(m.split('=').tail.head.replaceAll("'","").trim())
                                                                case None => None
                                                                },
                                                       SUBPARTITION_SPEC = x.SUBPARTITION match {
                                                                case Some(m) => Some(m.split('=').head.trim())
                                                                case None => None
                                                                },
                                                       SUBPARTITION_VALUE = x.SUBPARTITION match {
                                                                case Some(m) => Some(m.split('=').tail.head.replaceAll("'","").trim())
                                                                case None => None
                                                                },
                                                       NUM_RECORDS = x.NUM_RECORDS,
                                                       NUM_MISSING = x.NUM_MISSING,
                                                       NUM_VALID = x.NUM_VALID,
                                                       MEAN = x.MEAN,
                                                       STANDARD_DEVIATION = x.STANDARD_DEVIATION,
                                                       MIN = x.MIN,
                                                       MEDIAN = x.MEDIAN,
                                                       FIRST_QUARTILE = x.FIRST_QUARTILE,
                                                       THIRD_QUARTILE = x.THIRD_QUARTILE,
                                                       MAX = x.MAX,
                                                       NUM_APPROX_UNIQUE_VALUES = x.NUM_APPROX_UNIQUE_VALUES)).toDF()



   statsDf.show(100,false)*/

//   collector.saveStatistics(statsDf, ShapeMonitorConfig)

    // Thread.sleep(100)

  }

}
