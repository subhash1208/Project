package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{IncrementalExporterConfig, incrementalDataExporterOptions}
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.SparkFiles
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class incrementalDataExporterTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cleanAndSetupDatabase("testgreenraw")
    cleanAndSetupDatabase("testbluelanding")
    createTableFromResource("testbluelanding", s"optout_master", s"optout_master")
  }

  lazy val incrementalExporterConfig: IncrementalExporterConfig = {
    val dxrArgs = buildTestDxrArgs(s"RTW_DIT")
    val config  = incrementalDataExporterOptions.parse(dxrArgs)
    config
  }

  private def buildTestDxrArgs(
      dxrProductName: String,
      resourcePrefix: String = s"/postgres/oracle/"): Array[String] = {

    if (dxrProductName.endsWith(".sql")) {
      val dxrTargetQuerySQLPath =
        getPathByResourceName(dxrProductName, resourcePrefix)
      sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)
    }

    val extractNames = List("extract_test", "extract_tab_test")
    //    val extractNames     = List("extract_test")
    val extractFilePaths =
      extractNames.map(name => getPathByResourceName(s"$name.sql", resourcePrefix))

    // add files to sparkContext
    extractFilePaths.foreach(sparkSession.sparkContext.addFile)
    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    val dxrArgs =
      s"""--product-name $dxrProductName
      --dxr-jdbc-url $dxrJdbcURL
      --output-partition-spec db_schema
      --sql-files ${extractNames.mkString(",")}
      --files ${extractFilePaths.mkString(",")}
      --strict-mode true
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master none
      --maintenance-check true
      --oracle-wallet-location $oracleWalletPath
      --sql-conf HISTORY_MONTHS_START=201901
      --sql-conf HISTORY_MONTHS_END=201812
      --sql-conf env=DIT
      """.stripMargin.split("\\s+")
    dxrArgs
  }


  ignore("must run incremental exporter") {
    // TODO: this needs to be implemented
  }

}
