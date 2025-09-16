package com.adp.datacloud.ds

import com.adp.datacloud.cli.CubeConfig
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CubeBuilderTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override lazy implicit val sparkSession: SparkSession = getTestSparkSessionBuilder()
    .config("spark.sql.extensions", "com.adp.datacloud.ds.LimitedGroupingSetsExtension")
    .getOrCreate()

  override def beforeAll() = {
    super.beforeAll()
    setUpDatabase("testgreenraw")
    setUpDatabase("testgreenmain")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenmain")
  }

  lazy val cubeConfig = testConfigParsing

  def testConfigParsing(): CubeConfig = {
    val xmlConfFile     = "test_benchmark_cube_annual.xml"
    val xmlConfFilePath = getPathByResourceName(xmlConfFile)
    sparkSession.sparkContext.addFile(xmlConfFilePath)

    val inputSpecSql = getPathByResourceName("benchmark_core_annual_sample.parquet")

    val args =
      s"""
      --local-parquet-input-mode true  
      --input-spec $inputSpecSql
      --xml-config-file $xmlConfFile
      --output-db-name testgreenmain
      --max-depth 3
      --min-depth 0
      --output-partition-spec aggregation_depth
      --hiveconf environment=test
      --hiveconf __GREEN_RAW_DB__=testgreenraw
      --hiveconf __GREEN_BASE_DB__=testgreenbase
      --hiveconf __GREEN_MAIN_DB__=testgreenmain
      --hiveconf yyyymmdd=20200227 
      --hiveconf YYYYMMDD=20200227 
      --hiveconf yyyymmdd_lower=20200227 
      --hiveconf YYYYMMDD_LOWER=20200227 
      --hiveconf yyyymmdd_upper=20200227 
      --hiveconf YYYYMMDD_UPPER=20200227 
      --hiveconf yyyymmww=2020W08 
      --hiveconf YYYYMMWW=2020W08 
      --hiveconf yyyymmww_lower=2020W08 
      --hiveconf YYYYMMWW_LOWER=2020W08 
      --hiveconf yyyymmww_upper=2020W08 
      --hiveconf YYYYMMWW_UPPER=2020W08 
      --hiveconf ingest_month=202001 
      --hiveconf INGEST_MONTH=202001 
      --hiveconf yyyymm=201912 
      --hiveconf YYYYMM=201912 
      --hiveconf yyyymm_lower=201912 
      --hiveconf YYYYMM_LOWER=201912 
      --hiveconf yyyymm_upper=201912 
      --hiveconf YYYYMM_UPPER=201912 
      --hiveconf qtr=2019Q4 
      --hiveconf QTR=2019Q4 
      --hiveconf qtr_lower=2019Q4 
      --hiveconf QTR_LOWER=2019Q4 
      --hiveconf qtr_upper=2019Q4 
      --hiveconf QTR_UPPER=2019Q4
      """.stripMargin.trim().split("\\s+")

    val cubeConfig = com.adp.datacloud.cli.CubeOptions.parse(args)
    cubeConfig
  }

  test("compute cube with config") {
    CubeBuilder.computeCube(cubeConfig)
  }

  test("check cube systemprops") {
    CubeBuilder.setLimitedGroupingSetsProperties(
      cubeConfig.xmlConfigString,
      cubeConfig.minCubeDepth,
      cubeConfig.maxCubeDepth)
    val cubeProps = System.getProperties.asScala.filter(x => x._1.startsWith("cube."))
    assert(cubeProps.nonEmpty)
    cubeProps.foreach(println)
  }

  ignore("getLimitedGroupingSetsAnalyzeRule ") {
    val processor = new CubeProcessor(cubeConfig.xmlConfigString)

    println(s"baseDimensions:${processor.baseDimensions}")
    println(s"minCubeDepth:${cubeConfig.minCubeDepth}")
    println(s"maxCubeDepth:${cubeConfig.maxCubeDepth}")
    println(s"mandatoryDimensions:${processor.mandatoryDimensions}")

    println("*****complex**types********")
    println(s"groupingSets:${processor.groupingSets}")
    println(s"hierarchies:${processor.hierarchies}")
    println(s"xorBlacklists:${processor.xorBlacklists}")
    println(s"limitedDepthDimensions:${processor.limitedDepthDimensions}")

    println("*****complex**types**as**json******")
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    val groupingSetsJson           = write(processor.groupingSets)
    val hierarchiesJson            = write(processor.hierarchies)
    val xorBlacklistsJson          = write(processor.xorBlacklists)
    val limitedDepthDimensionsJson = write(processor.limitedDepthDimensions)

    println(s"groupingSetsJson:$groupingSetsJson")
    println(s"hierarchiesJson:$hierarchiesJson")
    println(s"xorBlacklistsJson:$xorBlacklistsJson")
    println(s"limitedDepthDimensionsJson:$limitedDepthDimensionsJson")

    println("*****complex**types**recovered******")

    val groupingSets           = read[List[String]](groupingSetsJson)
    val hierarchies            = read[List[List[String]]](hierarchiesJson)
    val xorBlacklists          = read[List[(String, Seq[String])]](xorBlacklistsJson)
    val limitedDepthDimensions = read[List[(String, Int)]](limitedDepthDimensionsJson)

    println(s"groupingSets:${groupingSets}")
    println(s"hierarchies:${hierarchies}")
    println(s"xorBlacklists:${xorBlacklists}")
    println(s"limitedDepthDimensions:${limitedDepthDimensions}")
  }

}
