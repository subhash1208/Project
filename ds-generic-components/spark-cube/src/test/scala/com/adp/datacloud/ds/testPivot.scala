package com.adp.datacloud.ds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSessionExtensions

object testPivot {

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0)
    println(prefix + " " + elapsed + " milliseconds")
    result
  }

  def main(args: Array[String]) {
    testCubePivot()
  }

  def testCubePivot() {

    val c = new CubeProcessor("src/test/resources/test-pivot.xml")

    val limitedGroupingSetsAnalyzeRule = new LimitedGroupingSetsAnalyzeRule(
      c.baseDimensions,
      0,
      0,
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

    val cubeProcessor = new CubeProcessor("src/test/resources/test-pivot.xml", None)

    val df = sparkSession.read.parquet("src/test/resources/bmfactors")
    df.filter("qtrs_since_last_promotion is not null and qtrs_since_last_promotion != 0")
      .show()
    //df.show()
    //println("Frame Size = " + df.count())

    time("Finished in")({
      val x = cubeProcessor.build(df)
      x.explain(true)
      x.show(10)
      assert(
        cubeProcessor.mandatoryDimensions.forall { y =>
          x.filter(col(y).isNull).count() == 0
        },
        "Error. Mandatory Dimension test failed in the cube")
      //c.dimensions.foreach { y => x.select(y).distinct().show() }
    })

  }

}
