package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.dxrParallelDataExporterOptions
import com.adp.datacloud.ds.util.DXR2Exception
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s.jackson.Serialization.writePretty
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DxrParallelDataExporterTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw")
    setUpDatabase("testblueraw")
    setUpDatabase("testbluelanding")
    createTableFromResource(
      "testgreenraw",
      "dxr_export_datacloud_test",
      "dxr-DATACLOUD-export.snappy.parquet")
  }

  // uncomment the below line to use localES for testing
  //  override val useLocalES: Boolean = true

  lazy val exportConfig = {
    val resourcePrefix: String = s"/dxrSqls/oracle/"
    val productName            = s"dxr_target_query.sql"
    val dxrTargetQuerySQLPath  = getPathByResourceName(productName, resourcePrefix)
    val sqlFilePaths = List("preExport.sql", "postExport.sql").map(
      getPathByResourceName(_, resourcePrefix))

    // add files to sparkContext
    (dxrTargetQuerySQLPath :: sqlFilePaths).foreach(sparkSession.sparkContext.addFile)
    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    val args =
      s"""
      --product-name $productName
      --dxr-jdbc-url $dxrJdbcURL
      --oracle-wallet-location $oracleWalletPath
      --application-name dxrParallelDataExporterTest
      --pre-export-sql preExport.sql
      --post-export-sql postExport.sql
      --hive-table testgreenraw.dxr_export_datacloud_test
      --target-rdbms-table [OWNERID].dxr_export_test
      --plsql-timeout 300
      --use-staging-table true
        """.trim().stripMargin.split("\\s+")

    dxrParallelDataExporterOptions.parse(args)
  }

  test("parse dxrExport args into config") {
    println(writePretty(exportConfig))
  }

  test("build connections df from config") {
    val connsDf = dxrParallelDataExporter.getDXRConnectionsDf(exportConfig)
    //    connsDf.show(false)
    connsDf
      .select(
        "CONNECTION_ID",
        "PASSWORD",
        "OWNER_ID",
        "TARGET_TABLE",
        "STG_TABLE",
        "CONN_STRING")
      .show(false)
  }

  test("should run export successfully") {
    sparkSession.catalog.listTables("testgreenraw").show(500, false)
    printSparkConfAsJson()
    try {
      dxrParallelDataExporter.runExport(exportConfig)
    } catch {
      case e: DXR2Exception => e.printStackTrace()
    }
  }

  ignore("should create table testgreenraw.extract_test ") {
    val dataDf = sparkSession.table("testgreenraw.extract_test")
    dataDf.write
      .mode(SaveMode.Overwrite)
      .jdbc(exportConfig.dxrJdbcUrl, "extract_test", exportConfig.dxrConnectionProperties)
  }

}
