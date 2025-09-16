package com.adp.datacloud.cli

import org.json4s.jackson.Serialization.{read, write}

abstract class BaseClass {
  val lazyVal: String
}

case class MyTestCaseClass(val saveFormat: String) extends BaseClass {
  override lazy val lazyVal: String = "hello"
}

object configJsonTest {

  def main(args: Array[String]) {
    val config           = MyTestCaseClass("saveFormat")
    implicit val formats = org.json4s.DefaultFormats
    println(config.lazyVal)
    val configJSON: String = write(config)
    println(configJSON)

    val cfgJSON = s"""
      {"dxrProductName":"dxr_target_query.sql","dxrJdbcUrl":"jdbc:oracle:thin:@dc1prtlf4-scan.whc.dc01.us.adp:1521/cri11p_svc1","dxrDatabaseUser":"ADPI_DXR","sourcedb":"oracle","outputDatabaseName":"dc1dsraw","blueDatabaseName":"dc1adsblueraw","inputParams":{"ENV":"PROD","HISTORY_MONTHS_START":"201903","HISTORY_MONTHS_END":"201906"},"outputPartitionSpec":"environment,db_schema,yyyymm","applicationName":"Candidate Relevancy Extracts","parallelizationColumns":["DB_SCHEMA"],"overwriteOutputTable":true,"sqlNamesList":["cr_xtrct_resume_attachments"],"batchColumn":"batch_number","maxParallelConnectionsPerDb":10,"parallelHiveTableWrites":2,"fetchArraySize":1000,"upperBound":200,"numPartitions":10,"cleanUpTmpFiles":true}
     """.trim.replaceFirst("outputDatabaseName", "greenDatabaseName")

    println(cfgJSON)

    // fix for deserialization, transient variables are not serialized but need a place holder during deserialization

    val placeHolders =
      List(("optoutMaster", "\"\""), ("dxrDatabasePassword", "\"\""), ("dxrParmas", "{}"))

    val configJSONString = placeHolders.foldLeft(cfgJSON)((cfgString, nextPair) => {
      if (cfgString.contains(nextPair._1)) {
        cfgString
      } else {
        val dummyPlaceHolder = s"""{"${nextPair._1}":${nextPair._2},""".trim
        val replacedJSON     = cfgString.trim.replaceFirst("^\\{", dummyPlaceHolder).trim
        replacedJSON
      }
    })

    println(configJSONString)

    val cgf = read[DxrParallelDataExtractorConfig](configJSONString)

    println(cgf)

  }

}
