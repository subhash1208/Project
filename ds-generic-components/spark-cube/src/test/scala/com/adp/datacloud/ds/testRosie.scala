package com.adp.datacloud.ds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions

object testRosie {

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0)
    println(prefix + " " + elapsed + " milliseconds")
    result
  }

  def main(args: Array[String]) {
    testRosieLogic()
  }

  def testRosieLogic() {

    val c = new CubeProcessor("src/test/resources/test-rosie.xml")

    val limitedGroupingSetsAnalyzeRule = LimitedGroupingSetsAnalyzeRule(
      c.baseDimensions,
      0,
      3,
      c.groupingSets,
      c.mandatoryDimensions,
      c.hierarchies)

    type ExtensionsBuilder = SparkSessionExtensions => Unit
    def create(builder: ExtensionsBuilder): ExtensionsBuilder = builder
    val extensions: ExtensionsBuilder = create { extensions =>
      extensions.injectResolutionRule(sparkSession => limitedGroupingSetsAnalyzeRule)
    }

    implicit val sparkSession = SparkSession.builder
      .withExtensions(extensions)
      .master("local")
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/tpcheckpoints")

    val timeCube = sparkSession.read.parquet("src/main/resources/t_dim_day_rollup")
    val cubeProcessor =
      new CubeProcessor("src/test/resources/test-rosie.xml", Some(timeCube))

    val df = sparkSession.read.parquet("src/test/resources/test_data")
    //val df = spark.read.parquet("src/test/resources/r_dw_waf_test").sample(false, 0.003)
    df.show()
    println("Frame Size = " + df.count())
    //111
    time("Finished in")({
      val x = cubeProcessor.build(df)
      //x._2.explain(true)
      println(x.count())
      //println("Count= " + x._2.count());
    })

  }

}
