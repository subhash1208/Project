package com.adp.datacloud.ds

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSessionExtensions

object testCases {

  val c = new CubeProcessor("src/test/resources/test-hierarchy_only.xml")

  val limitedGroupingSetsAnalyzeRule = new LimitedGroupingSetsAnalyzeRule(
    c.baseDimensions,
    0,
    3,
    c.groupingSets,
    c.mandatoryDimensions,
    c.hierarchies,
    c.xorBlacklists)

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

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[X](ys: Traversable[X]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0)
    println(prefix + " " + elapsed + " milliseconds")
    result
  }

  def rollups(xs: List[String]): List[List[String]] = {
    xs match {
      case List() => List(List[String]())
      case str :: tail => {
        val tailResult = rollups(tail)
        List(List[String]()) ++ tailResult.map { x => List(str) ++ x }
      }
    }
  }

  def main(args: Array[String]) {
    testSimpleCubeBuild()
    //testDualHierarchy()
    println("sleeping")
    //Thread.sleep(1000000) // To hold the local WebUI for inspection
  }

  def testDualHierarchy() {
    val h1 = List("a", "b", "c")
    val h2 = List("x", "c")
    val r1 = rollups(h1)
    val r2 = rollups(h2)

    val cross =
      rollups(h1).cross(rollups(h2)).map({ case (x, y) => (x ++ y).toSet.toList }).toList
    val listOfRollups = List(r1, r2)
    val cross2 = listOfRollups.tail.foldLeft(listOfRollups.head)({
      (acc: List[List[String]], b: List[List[String]]) =>
        {
          (acc.cross(b)).toList.map({ case (x, y) => (x ++ y).toSet.toList })
        }
    }) //.map (_.sorted) toList

    println("***")
    cross2.foreach(println)
  }

  def testSimpleCubeBuild() {

    println("****")
    println(c.baseDimensions)
    println(c.mandatoryDimensions)
    println(c.hierarchies)
    println(c.xorBlacklists)
    println("****")

    val q = sparkSession
      .createDataFrame(
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
                  "AP",
                  "AP",
                  "TN",
                  "TX",
                  "TX",
                  "TS",
                  "TS",
                  "CA",
                  "TN",
                  "AP",
                  "AP",
                  "TS",
                  "NJ",
                  "TX"),
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
            StructField("value", IntegerType, nullable       = true),
            StructField("l2_code", StringType, nullable      = true),
            StructField("reg_temp_dsc", StringType, nullable = true),
            StructField("clnt_obj_id", StringType, nullable  = true),
            StructField("state", StringType, nullable        = true),
            StructField("country", StringType, nullable      = true))))
      .withColumn("region", col("country"))
      .withColumn("subregion", col("country"))

    println("Original Data")
    //q.show()

    val x = c.build(q)
    x.show()

  }

}
