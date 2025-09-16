package com.adp.datacloud.ds

import org.apache.log4j.Logger

object testStatisticsCollector {

  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]) {

    // Collect statistics for the tables
    /* val df = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")
    val m = df.schema.fields

    val columnList = m.map( x  => (x.name,x.dataType))


   columnList.foreach(print)*/

    val testMonitor = new ShapeMonitorTest()
//    testMonitor.testStatsCollection

  }

}
