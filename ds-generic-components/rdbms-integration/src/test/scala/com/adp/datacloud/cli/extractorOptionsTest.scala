package com.adp.datacloud.cli

import org.apache.log4j.Logger

object extractorOptionsTest {

  val logger = Logger.getLogger(getClass)

  val extractorOptions = Array(
    "--input-spec",
    "T_FACT_WORK_ASGNMT",
    "--source-db-host",
    "cdlpfthf6-scan.es.ad.adp.com",
    "--source-db-sid",
    "cri11y_svc1",
    "--source-db-username",
    "ADPIDMCORE01",
    "--source-db-password",
    "adpiv2",
    "--output-hive-db",
    "IGNORED",
    "--output-hive-table",
    "IGNORED")

  def main(args: Array[String]) {
    val sql = testNoBatchSql()
    testStringBatchSql(sql)
    testNumericBatchSql()
    testStringBatchFilteredSql(sql)
  }

  def testNoBatchSql() = {
    val config = dxrParallelDataExtractorOptions.parse(extractorOptions)
    println(config.sqlStringsMap)
    config.sqlStringsMap.mkString(",")
  }

  def testStringBatchSql(sql: String) = {
    val optionArgs = Array("--batch-column", "clnt_obj_id")
    val config     = dxrParallelDataExtractorOptions.parse(extractorOptions ++ optionArgs)
    println(config.sqlStringsMap)
  }

  def testNumericBatchSql() = {
    val optionArgs = Array("--batch-column", "annl_cmpn_amt")
    val config     = dxrParallelDataExtractorOptions.parse(extractorOptions ++ optionArgs)
    println(config.sqlStringsMap)
  }

  def testStringBatchFilteredSql(sql: String) = {
    val optionArgs = Array(
      "--batch-column",
      "clnt_obj_id",
      "--static-batch-filters",
      "05QRX003FE500004,G33N94Y6E1XGRJTE,G3FKZ06R66RTZDT9")
    val config = dxrParallelDataExtractorOptions.parse(extractorOptions ++ optionArgs)
    println(config.sqlStringsMap)
  }

}
