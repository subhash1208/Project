package com.adp.datacloud.ds.udafs

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.annotation.tailrec

/**
 * UDAF to compute approximate quantile.
 * Trades in more accuracy for speed of computation compared to Greenwald Khanna algoritm.
 * There are many other alternatives like T-Digest, and native quantile support in spark 1.6 and 2.0
 * This one would be the fastest of the lot - at the cost of accuracy. 
 * Whenever the underlying size of elements is less than 100, the nearest rank method is used inherently.
 * https://en.wikipedia.org/wiki/Percentile#The_Nearest_Rank_method 
 *
 * This would need to be rewritten in Spark 2.0
 * http://stackoverflow.com/questions/32100973/how-to-define-and-use-a-user-defined-aggregate-function-in-spark-sql
 * http://stackoverflow.com/questions/28332494/querying-spark-sql-dataframe-with-complex-types
 *
 */
@Deprecated
class RoundedApproximatePercentile(
    percentiles: Seq[Int], // The list of percentiles to be calculated
    binWidth: Double = 50,
    minimum: Double = Double.NegativeInfinity,
    maximum: Double = Double.PositiveInfinity) extends UserDefinedAggregateFunction {

  require(binWidth > 0, "Bin Width MUST be positive")
  require(percentiles.forall { x => x > 0 && x < 100 }, "")

  def inputSchema: StructType = StructType(Array(
    StructField("value", DoubleType) // The column on which percentiles have to be calculated
    ))

  def deterministic = true

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("histogram", MapType(LongType, LongType)), // contains map of (x,y) where x = bin_sequence and y = num_elements_in_bin
    StructField("numelements", LongType)))

  // Returned Data Type .
  def dataType: DataType = ArrayType(DoubleType, true)

  // Called whenever the key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = Map[Long, Long]() // Empty Map
    buffer(1) = 0L
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
        
    if (input != null && !input.isNullAt(0)) {
      // Update the histogram with this element
      val value = if (input.getDouble(0) < minimum) minimum else if (input.getDouble(0) > maximum) maximum else input.getDouble(0)
      val key = (value / binWidth).toLong
      buffer(0) = buffer.getAs[Map[Long, Long]](0) ++ Map[Long, Long](key -> ((buffer.getAs[Map[Long, Long]](0)).getOrElse[Long](key, 0) + 1))
  
      // Increase the total number of elements in the histogram
      buffer(1) = buffer.getLong(1) + 1
      
    }

  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    
    // Merge the two histograms
    buffer1(0) = buffer1.getAs[Map[Long, Long]](0) ++ buffer2.getAs[Map[Long, Long]](0).map {
      case (key, value) => key -> (value + buffer1.getAs[Map[Long, Long]](0).getOrElse[Long](key, 0))
    }

    // Sum the count of elements in both the histograms
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    
  }

  // Called after all the entries are exhausted.
  @Override
  def evaluate(buffer: Row) = {
    
    val numElements = buffer.getLong(1)
    // sort the histogram created so far by keys
    val histogram = scala.collection.immutable.TreeMap(buffer.getAs[Map[Long, Long]](0).toArray:_*)
    // println(histogram)
    val percentileValues: Array[Double] = percentiles.map {
      x =>
        {
          val index = Math.round(x * numElements / 100)

          @tailrec
          def getBinLowerBound(hist: Map[Long, Long], index: Long): Long = {
            if (hist.isEmpty || hist.head._2 >= index) {
              hist.head._1
            } else {
              getBinLowerBound(hist.tail, index - hist.head._2)
            }
          }

          // return the lower bound of the bin 
          if(histogram.isEmpty) Double.NaN else getBinLowerBound(histogram, index) * binWidth

        }
    } toArray
    
    percentileValues
    //new GenericArrayData(percentileValues)
    
  }

}

object approximatePercentileTest {

  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("ApproximateQuantileTest").enableHiveSupport()
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    val data = List(10, 29, 15, 43, 9, 12, null, 13, 16, 9, 16, null, 9, 10) zip
      List[String]("A", "A", "A", "B", "A", "B", "A", "A", "A", "B", "A", "B", "B", "B") toList
      
    val data2 = List(66.66666666666666, 75.0, 80.0, 0.0, 33.33333333333333, 100.0, 0, 33.33333333333333, 50) zip
      List[String]("B", null, "A", "B", null, "A", "B", null, "A") toList

    val q = sparkSession.createDataFrame(
      sc.parallelize(
        data2.map { x => Row(x._1, x._2) }),
      StructType(List(
        StructField("value", DoubleType, nullable = true),
        StructField("type", StringType, nullable = true))))

    q.show()

    // define UDAF
    // val myPercentile = new RoundedApproximatePercentile(List(90, 60), 2)
    val myPercentile = new RoundedApproximatePercentile(List(30, 70), 2)

    val result = q.groupBy("type").agg(myPercentile(col("value")).as("percentiles"))
    
    //result.select($"type", col("percentiles").getItem(0)).show();

    result.show()
    
  }
}