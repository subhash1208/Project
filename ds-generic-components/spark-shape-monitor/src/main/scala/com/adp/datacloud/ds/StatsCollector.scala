package com.adp.datacloud.ds

import org.apache.spark.sql.{ DataFrame, SparkSession }

trait StatsCollector {

  // collect statistics incrementally and return statistics for all applicable columns
  def collect() : DataFrame
  
}
