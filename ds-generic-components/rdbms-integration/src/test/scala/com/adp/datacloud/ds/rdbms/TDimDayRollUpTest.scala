package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.dxrParallelDataExtractorOptions
import com.adp.datacloud.ds.rdbms.dxrParallelDataExtractor.runExtract
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TDimDayRollUpTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override def beforeAll() = {
    super.beforeAll()
    setUpDatabase("testgreenraw", true)
    setUpDatabase("testblueraw", true)
    setUpDatabase("testbluelanding", true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenraw")
    displayAllTables("testblueraw")
    displayAllTables("testbluelanding")
  }

  ignore("generate rollup data ") {
    val resourcePrefix: String = s"/dxrSqls/oracle/"
    val dxrProductName         = "dxr_meta_model_target_query.sql"
    val dxrTargetQuerySQLPath  = getPathByResourceName(dxrProductName, resourcePrefix)
    sparkSession.sparkContext.addFile(dxrTargetQuerySQLPath)

    val extractNames = List("t_dim_day_rollup", "t_dim_day")
//    val extractNames     = List("t_dim_day")
    val extractFilePaths =
      extractNames.map(name => getPathByResourceName(s"$name.sql", resourcePrefix))

    // add files to sparkContext
    extractFilePaths.foreach(sparkSession.sparkContext.addFile)
    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    val dxrArgs =
      s"""--product-name $dxrProductName
      --dxr-jdbc-url $dxrJdbcURL
      --sql-files ${extractNames.mkString(",")}
      --files ${extractFilePaths.mkString(",")}
      --strict-mode true
      --green-db testgreenraw
      --blue-db testblueraw
      --landing-db testbluelanding
      --optout-master none
      --maintenance-check true
      --oracle-wallet-location $oracleWalletPath
      --sql-conf HISTORY_MONTHS_START=2001
      --sql-conf HISTORY_MONTHS_END=2022
      --sql-conf env=DIT
      """.stripMargin.split("\\s+")

    val config     = dxrParallelDataExtractorOptions.parse(dxrArgs)
    val connDS     = dxrParallelDataExtractor.getConnectionsDfFromConfig(config)
    val connection = connDS.head()
    println(connection)
    runExtract(config)
  }

}
