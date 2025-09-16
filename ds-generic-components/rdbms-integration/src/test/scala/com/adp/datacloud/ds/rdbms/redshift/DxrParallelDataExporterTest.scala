package com.adp.datacloud.ds.rdbms.redshift

import com.adp.datacloud.cli.dxrParallelDataExporterOptions
import com.adp.datacloud.ds.rdbms.dxrParallelDataExporter
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s.jackson.Serialization.writePretty
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class DxrParallelDataExporterTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  val TABLE_TO_EXPORT = "dxr_redshift_export_datacloud_test"
  val RESOURCE_PREFIX =
    s"${File.separator}dxrSqls${File.separator}redshift${File.separator}"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpDatabase("testgreenraw")
    setUpDatabase("testblueraw")
    setUpDatabase("testbluelanding")

    createTableFromResource(
      "testgreenraw",
      TABLE_TO_EXPORT,
      "dxr_export_test.snappy.parquet")
  }

  // uncomment the below line to use localES for testing
  //  override val useLocalES: Boolean = true

  lazy val exportConfig = {
    val productName           = "dxr_redshift_target_query.sql"
    val dxrTargetQuerySQLPath = getPathByResourceName(productName, RESOURCE_PREFIX)
    val sqlFilePaths = List("pre_export.sql", "post_export.sql").map(sqlName =>
      getPathByResourceName(sqlName, RESOURCE_PREFIX))

    // add files to sparkContext
    (dxrTargetQuerySQLPath :: sqlFilePaths).foreach(sparkSession.sparkContext.addFile)
    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    val args =
      s"""
      --product-name $productName
      --dxr-jdbc-url $dxrJdbcURL
      --target-db redshift
      --oracle-wallet-location $oracleWalletPath
      --application-name dxrParallelDataExporterTest
      --pre-export-sql pre_export.sql
      --post-export-sql post_export.sql
      --hive-table testgreenraw.$TABLE_TO_EXPORT
      --target-rdbms-table [OWNERID].dxr_redshift_export_test
      --plsql-timeout 300
      --use-staging-table true
      --redshift-iam-role arn:aws:iam::425932292478:role/adpa-ng-etl-role
      --redshift-s3-tempdir s3://datacloud-datascience-dxr-staging-blue-708035784431tf/redshift/test
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
    dxrParallelDataExporter.runExport(exportConfig)
  }

  ignore("should create table testgreenraw.extract_test ") {
    val dataDf = sparkSession.table("testgreenraw.extract_test")
    dataDf.write
      .mode(SaveMode.Overwrite)
      .jdbc(exportConfig.dxrJdbcUrl, "extract_test", exportConfig.dxrConnectionProperties)
  }

}
