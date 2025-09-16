package com.adp.datacloud.ds

import com.adp.datacloud.util.SparkTestWrapper
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileApprox
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class CubeProcessorTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override def beforeAll() = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override lazy implicit val sparkSession: SparkSession = getTestSparkSessionBuilder()
    .config("spark.sql.extensions", "com.adp.datacloud.ds.LimitedGroupingSetsExtension")
    .getOrCreate()

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0)
    println(prefix + " " + elapsed + " milliseconds")
    result
  }

  def getXmlAsString(fileName: String): String = {
    val xmlSource = Source.fromFile(s"src/test/resources/$fileName")
    val xmlString =
      try xmlSource.mkString
      finally xmlSource.close()
    xmlString
  }

  test("test cube analyzer") {
    val xmlString = getXmlAsString("test_cube_analyzer.xml")

    CubeBuilder.setLimitedGroupingSetsProperties(
      xmlString,
      minCubeDepth = 0,
      maxCubeDepth = 3)

    lazy val sc = sparkSession.sparkContext

    sc.setCheckpointDir("/tmp/tpcheckpoints")

    val timeCube = sparkSession.read.parquet(
      "src/main/resources/t_dim_day_rollup/t_dim_day_rollup.snappy.parquet")

    timeCube.show(5)
    val cubeProcessor = new CubeProcessor(xmlString, Some(timeCube))

    val df = sparkSession.read
      .parquet("src/test/resources/r_dw_waf_test_100")
      .withColumn("dummy_level1", lit("1"))
      .withColumn("dummy_level2", lit("1"))
      .withColumn("dummy_level3", lit("1"))
      .withColumn("dummy_level4", lit("1"))
      .withColumn("dummy_level5", lit("1"))
      .withColumn("dummy_level6", lit("1"))
      .withColumn("dummy_level7", lit("1"))
      .withColumn("dummy_level8", lit("1"))
      .withColumn("dummy_level9", lit("1"))
      .withColumn("dummy_level10", lit("1"))

    df.show()

    val x = time("Finished in")({
      cubeProcessor.build(df)
    })

    //println("COUNT = " + x._2.count()) //111
    x.show()
  }

  test("test cube depth") {

    val xmlString = getXmlAsString("test_cube_depth.xml")

    CubeBuilder.setLimitedGroupingSetsProperties(
      xmlString,
      minCubeDepth = 0,
      maxCubeDepth = 3)

    lazy val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/tpcheckpoints")

    val timeCube = sparkSession.read.parquet(
      "src/main/resources/t_dim_day_rollup/t_dim_day_rollup.snappy.parquet")
    timeCube.show(5)
    val cubeProcessor =
      new CubeProcessor(xmlString, Some(timeCube))

    val df = sparkSession.read.parquet("src/test/resources/r_dw_waf_test_100")
    df.groupBy("clnt_obj_id", "d_pay_rt_type_cd").count().explain(false)

    df.show()
    println("Frame Size = " + df.count())
    //111
    time("Finished in")({
      val x = cubeProcessor.build(df)
      //      println(x._2.queryExecution.optimizedPlan)
      x.show()
      x.explain(false)
      println(x.count())
      println("Count= " + x.count());
    })

  }

  test("test groupingids") {

    val xmlString = getXmlAsString("test-grouping__id.xml")

    CubeBuilder.setLimitedGroupingSetsProperties(
      xmlString,
      minCubeDepth = 0,
      maxCubeDepth = 3)

    lazy val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/tpcheckpoints")

    val q = sparkSession.createDataFrame(
      sc.parallelize(
          (
            (
              List(10, 29, 15, 43, 9, 12, null, 13, 16, 15, 16, null, 16, 15),
              List[String](
                "A",
                "A",
                "A",
                "B",
                "A",
                "B",
                "A",
                "A",
                "A",
                "B",
                "A",
                "B",
                "B",
                "B"),
              List[String](
                "X",
                "X",
                "X",
                "X",
                "Y",
                "Y",
                "Y",
                "X",
                "X",
                "X",
                "X",
                "Y",
                "Y",
                "Y")).zipped.toList,
            (
              List[String](
                "X1",
                "X1",
                "X2",
                "X1",
                "X1",
                "X3",
                "X3",
                "X3",
                "X1",
                "X1",
                "X1",
                "X2",
                "X2",
                "X1"),
              List[String](
                "2016",
                "2016",
                "2016",
                "2016",
                "2015",
                "2015",
                "2015",
                "2015",
                "2015",
                "2015",
                "2015",
                "2017",
                "2017",
                "2017"),
              List[String](
                "2016_1",
                "2016_1",
                "2016_1",
                "2016_2",
                "2015_1",
                "2015_1",
                "2015_1",
                "2015_4",
                "2015_4",
                "2015_4",
                "2015_4",
                "2017_3",
                "2017_3",
                "2017_2")).zipped.toList).zipped.toList.map(x =>
            (x._1._1, x._1._2, x._1._3, x._2._1, x._2._2, x._2._3)))
        .map { x => Row(x._1, x._2, x._3, x._4, x._5, x._6) },
      StructType(
        List(
          StructField("value", IntegerType, nullable       = true),
          StructField("l2_code", StringType, nullable      = true),
          StructField("reg_temp_dsc", StringType, nullable = true),
          StructField("clnt_obj_id", StringType, nullable  = true),
          StructField("yr", StringType, nullable           = true),
          StructField("qtr", StringType, nullable          = true))))

    println("Original Data")
    q.show()

    val fullCube = q
      .cube("clnt_obj_id", "l2_code", "reg_temp_dsc")
      .agg(col("grouping__id"), count("value"))
    val depthRestrictedCube = q
      .cube("clnt_obj_id", "l2_code", "reg_temp_dsc")
      .agg(
        col("grouping__id"),
        count("value"),
        sum("value")
      ) //.filter("grouping__id >= 4")
    //depthRestrictedCube.explain(true)
    depthRestrictedCube.show(500)
    //depthRestrictedCube.queryExecution.logical.foreach { x => }

    //fullCube.explain(true)
    //depthRestrictedCube.explain(true)
    /*q.groupBy(col("clnt_obj_id"), col("l2_code"), lit(null.asInstanceOf[String]).as("reg_temp_dsc")).count().unionAll(
        q.groupBy(col("clnt_obj_id"), lit(null.asInstanceOf[String]).as("l2_code"), col("reg_temp_dsc")).count()
        ).explain(true)*/

    //cubeProcessor.build(q)(0)._2.show()

  }

  test("test grouping sets") {

    val xmlString = getXmlAsString("test-grouping_sets.xml")

    CubeBuilder.setLimitedGroupingSetsProperties(
      xmlString,
      minCubeDepth = 0,
      maxCubeDepth = 3)

    val cubeProcessor = new CubeProcessor(xmlString)

    lazy val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/tpcheckpoints")

    val df = sparkSession.read.parquet("src/test/resources/r_dw_waf_test")
    println(cubeProcessor.groupingSets)
    println("Frame Size = " + df.count())

    sparkSession.sqlContext.sql(
      s"CREATE OR REPLACE TEMPORARY FUNCTION hive_percentile_approx AS '" +
        s"${classOf[GenericUDAFPercentileApprox].getName}'")

    time("Finished in")({
      val x = cubeProcessor.build(df)
      //println(x._2.explain(true))
      x.show(20)
      println("Count of grouping sets result = " + x.count())
    })

  }

  test("test SysCalendarDays") {

    val xmlString = getXmlAsString("test-sys_calendar_days.xml")

    CubeBuilder.setLimitedGroupingSetsProperties(
      xmlString,
      minCubeDepth = 0,
      maxCubeDepth = 3)

    lazy val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/tpcheckpoints")

    val q = sparkSession.createDataFrame(
      sc.parallelize(
          (
            (
              List(10, 29, 15, 43, 9, 12, null, 13, 16, 15, 16, null, 16, 15),
              List[String](
                "A",
                "A",
                "A",
                "B",
                "A",
                "B",
                "A",
                "A",
                "A",
                "B",
                "A",
                "B",
                "B",
                "B"),
              List[Int](2015, 2015, 2015, 2015, 2016, 2016, 2016, 2016, 2017, 2017, 2014,
                2014, 2014, 2014)).zipped.toList,
            (
              List[Int](20151, 20152, 20153, 20154, 20161, 20162, 20163, 20164, 20171,
                20172, 20141, 20142, 20143, 20144),
              List[Int](201503, 201505, 201508, 201511, 201602, 201604, 201607, 201612,
                201701, 201706, 201402, 201405, 20149, 201410),
              List[String](
                "IN",
                "IN",
                "IN",
                "US",
                "US",
                "IN",
                "IN",
                "US",
                "IN",
                "IN",
                "IN",
                "IN",
                "US",
                "US")).zipped.toList).zipped.toList.map(x =>
            (x._1._1, x._1._2, x._1._3, x._2._1, x._2._2, x._2._3)))
        .map { x => Row(x._1, x._2, x._3, x._4, x._5, x._6) },
      StructType(
        List(
          StructField("value", IntegerType, nullable   = true),
          StructField("l2_code", StringType, nullable  = true),
          StructField("yr_cd", IntegerType, nullable   = true),
          StructField("qtr_cd", IntegerType, nullable  = true),
          StructField("mnth_cd", IntegerType, nullable = true),
          StructField("country", StringType, nullable  = true))))

    val cubeProcessor = new CubeProcessor(xmlString)

    println("Original Data")
    q.show()

    val x = cubeProcessor.build(q)
    println("Total Cube Size = " + x.count())
    x.show(100);
  }

}
