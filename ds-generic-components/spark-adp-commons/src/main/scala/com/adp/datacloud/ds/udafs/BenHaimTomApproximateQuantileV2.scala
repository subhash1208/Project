package com.adp.datacloud.ds.udafs

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.annotation.tailrec

/**
 * UDAF to compute approximate quantile using Ben-Haim Tom method.
 * http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 * If the underlying unique values are less than n (default 10000), this method returns
 * an exact quantile, else an approximate quantile is returned.
 *
 * This class replicates the functionality of hive's default percentile_approx method.
 *
 * https://gist.github.com/sadikovi/7608c8c7eb5d7fe69a1a
 *
 * @author Manoj Oleti
 *
 * @param percentiles	The list of percentiles to be calculated
 * @param p						Parameter to control the accuracy of the approximation. Default 10000. If the number of unique values is less than this number, an exact percentile is returned
 * @param approximationFunction An approximation function applied on all inputs.
 * @param e 					Acceptable Error. (If e = 1, the inputs are rounded off to nearest integers)
 *
 */
class BenHaimTomApproximatePercentileV2(
  percentiles:           Seq[Int],
  p:                     Int              = 10000,
  approximationFunction: Double => Double = x => x // Use no approximation by default
) extends UserDefinedAggregateFunction {

  // Alternate constructor where the approximation function is the "round to nearest integer"
  def this(percentiles: Seq[Int], p: Int, roundToNearestInteger: Boolean) {
    this(percentiles, p, if (roundToNearestInteger) { (x: Double) => Math.round(x).toDouble } else { (x: Double) => x })
  }

  // Alternate constructor where the approximation function is expressed as the maximum allowable error
  // A value of 0.1 means that we can round off to the nearest decimal point
  def this(percentiles: Seq[Int], p: Int, error: Double) {
    this(percentiles, p, x => ((x * 1 / error).round * error))
  }

  require(p >= 10 && p <= 10000, "Parameter (p) must be between 10 and 10000")
  require(percentiles.forall { x => x > 0 && x < 100 }, "")

  def inputSchema: StructType = StructType(Array(
    StructField("value", DoubleType) // The column on which percentiles have to be calculated
  ))

  def deterministic = true

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("histogram_keys", DoubleBufferType),
    StructField("histogram_values", LongBufferType) // represents occurrence count
  ))

  // Returned Data Type .
  def dataType: DataType = ArrayType(DoubleType, true)

  // Called whenever the key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = new WrappedArrayBuffer[Double]()
    buffer(1) = new WrappedArrayBuffer[Long]()
  }

  /**
   * This method returns a tuple with two options (index, lastIndexLessThanKey)
   * If the key is found in the histogram then index will the index of the key in the histogram
   * If the key is not found in the histogram, then lastIndexLessThanKey holds the greatest index of the histogram where the value is less than the key
   */
  @tailrec
  private final def getNearestIndices(histogram: WrappedArrayBuffer[Double], key: Double,
                                      startIndex: Int, endIndex: Int): (Option[Int], Option[Int]) = {

    if (histogram.size == 0 || startIndex < 0) {
      (None, None)
    } else if (startIndex > endIndex) {
      // Search finished. Key not found
      (None, Some(endIndex))
    } else {
      // Do a recursive binary search
      val midIndex = startIndex + (endIndex - startIndex) / 2
      if (histogram(midIndex) == key) {
        (Some(midIndex), Some(midIndex))
      } else if (histogram(midIndex) > key) {
        getNearestIndices(histogram, key, startIndex, midIndex - 1)
      } else {
        getNearestIndices(histogram, key, midIndex + 1, endIndex)
      }
    }
  }

  // function with side-effects
  // merges the closest elements
  private def mergeClosestElements(
    histogramKeys:   WrappedArrayBuffer[Double],
    histogramValues: WrappedArrayBuffer[Long]) {
    var i = 1;
    var closestElementIndex = 0;
    var closestDistance = histogramKeys(1) - histogramKeys(0)
    while (i < histogramKeys.size - 1) {
      if (histogramKeys(i + 1) - histogramKeys(i) < closestDistance) {
        closestDistance = histogramKeys(i + 1) - histogramKeys(i)
        closestElementIndex = i
      }
      i += 1
    }

    val newKey = approximationFunction(
      (histogramKeys(closestElementIndex) * histogramValues(closestElementIndex) +
        histogramKeys(closestElementIndex + 1) * histogramValues(closestElementIndex + 1)) / (histogramValues(closestElementIndex) + histogramValues(closestElementIndex + 1)))
    val newValue = histogramValues(closestElementIndex) + histogramValues(closestElementIndex + 1)
    histogramKeys.merge(closestElementIndex, newKey)
    histogramValues.merge(closestElementIndex, newValue)
  }

  // Add element to the histogram
  private def add(
    histogramKeys:    WrappedArrayBuffer[Double],
    histogramValues:  WrappedArrayBuffer[Long],
    maxHistogramSize: Int,
    key:              Double,
    value:            Long                       = 1): (WrappedArrayBuffer[Double], WrappedArrayBuffer[Long]) = {

    val indices = getNearestIndices(histogramKeys, key, 0, histogramKeys.size - 1)
    indices._1 match {
      case Some(x) => {
        // Key found in the histogram
        histogramValues.update(x, histogramValues(x) + value)
        (histogramKeys, histogramValues)
      }
      case None => {
        // Key Not found in the histogram
        val splitIndex = indices._2 match {
          case Some(y) => y
          case None    => -1
        }
        histogramKeys.insertAt(splitIndex + 1, key)
        histogramValues.insertAt(splitIndex + 1, value)

        if (histogramKeys.size > maxHistogramSize) {
          // The histogram has become full. So, merge the two nearest keys
          mergeClosestElements(histogramKeys, histogramValues)
        }

        (histogramKeys, histogramValues)

      }
    }

  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {

    if (input != null && !input.isNullAt(0) && !input.getDouble(0).isNaN()) {

      val histogramKeys = buffer(0).asInstanceOf[WrappedArrayBuffer[Double]]
      val histogramValues = buffer(1).asInstanceOf[WrappedArrayBuffer[Long]]
      val currentKey = approximationFunction(input.getDouble(0))

      val updatedHistogramTuple = add(histogramKeys, histogramValues, p, currentKey)

      buffer(0) = updatedHistogramTuple._1
      buffer(1) = updatedHistogramTuple._2

    }

  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {

    val thisHistogramKeys = buffer1(0).asInstanceOf[WrappedArrayBuffer[Double]]
    val thisHistogramValues = buffer1(1).asInstanceOf[WrappedArrayBuffer[Long]]

    val thatHistogramKeys = buffer2(0).asInstanceOf[WrappedArrayBuffer[Double]]
    val thatHistogramValues = buffer2(1).asInstanceOf[WrappedArrayBuffer[Long]]

    // Efficiently merge the two histograms
    // Below code is written in an imperative fashion for performance reasons
    val mergedHistogramKeys = new WrappedArrayBuffer[Double](thisHistogramKeys.size + thatHistogramKeys.size)
    val mergedHistogramValues = new WrappedArrayBuffer[Long](thisHistogramValues.size + thatHistogramValues.size)

    // Iterate linearly and create a new array
    var i = 0
    var thisIndex = 0
    var thatIndex = 0
    while (thisIndex + thatIndex < thisHistogramKeys.size + thatHistogramKeys.size) {
      if (thisIndex == thisHistogramKeys.size) {
        // Finished iterating over "this" histogram
        mergedHistogramKeys(i) = thatHistogramKeys(thatIndex)
        mergedHistogramValues(i) = thatHistogramValues(thatIndex)
        thatIndex += 1
      } else if (thatIndex == thatHistogramKeys.size) {
        // Finished iterating over "that" histogram
        mergedHistogramKeys(i) = thisHistogramKeys(thisIndex)
        mergedHistogramValues(i) = thisHistogramValues(thisIndex)
        thisIndex += 1
      } else if (thisHistogramKeys(thisIndex) == thatHistogramKeys(thatIndex)) {
        mergedHistogramKeys(i) = thisHistogramKeys(thisIndex)
        mergedHistogramValues(i) = thisHistogramValues(thisIndex) + thatHistogramValues(thatIndex)
        thisIndex += 1
        thatIndex += 1
      } else if (thisHistogramKeys(thisIndex) > thatHistogramKeys(thatIndex)) {
        mergedHistogramKeys(i) = thatHistogramKeys(thatIndex)
        mergedHistogramValues(i) = thatHistogramValues(thatIndex)
        thatIndex += 1
      } else {
        mergedHistogramKeys(i) = thisHistogramKeys(thisIndex)
        mergedHistogramValues(i) = thisHistogramValues(thisIndex)
        thisIndex += 1
      }
      i += 1
    }

    // Trim the histogram down to size p
    while (mergedHistogramKeys.size > p) {
      mergeClosestElements(mergedHistogramKeys, mergedHistogramValues)
    }

    buffer1(0) = mergedHistogramKeys
    buffer1(1) = mergedHistogramValues

  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {

    val histogramKeys = buffer(0).asInstanceOf[WrappedArrayBuffer[Double]].underlyingAny.map(_.asInstanceOf[Double])
    val histogramValues = buffer(1).asInstanceOf[WrappedArrayBuffer[Long]].underlyingAny.map(_.asInstanceOf[Long])

    if (histogramKeys.isEmpty) {
      Array.emptyDoubleArray
    } else {

      val numElements = histogramValues.reduce(_ + _)
      val result = percentiles.map({
        x =>
          {

            @tailrec
            def getKeyForNthValue(histogramKeys: Array[Double], histogramValues: Array[Long], nthValue: Double, prevBinValue: Option[Double] = None): Double = {

              if (histogramValues.head >= nthValue) {
                prevBinValue match {
                  case Some(x) => x + ((histogramKeys.head - x) * nthValue / histogramValues.head)
                  case None    => histogramKeys.head
                }
              } else
                getKeyForNthValue(histogramKeys.tail, histogramValues.tail, nthValue - histogramValues.head, Some(histogramKeys.head))

            }

            getKeyForNthValue(histogramKeys, histogramValues, x * numElements / 100D)

          }
      }) toArray

      result

    }
    //new GenericArrayData(percentileValues)

  }

}

object benHaimTomApproximatePercentileV2Test {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("ApproximateQuantileTest").enableHiveSupport()
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    val data = List[Double](10, 29, 15, 43, 9, 12, null.asInstanceOf[Double], 13, 16, 9, 16, null.asInstanceOf[Double], 9, 10) zip
      List[String]("A", "A", "A", "B", "A", "B", "A", "A", "A", "B", "A", "B", "B", "B") toList

    val data2 = List(66.66666666666666, 75.0, 80.0, 0.0, 33.33333333333333, 100.0, 0, 33.33333333333333, 50) zip
      List[String]("B", null, "A", "B", null, "A", "B", null, "A") toList

    val data3 = List[Double](0.1, 0.29, 0.15, 0.43, 0.9, 1.2, null.asInstanceOf[Double], 1.3, 1.6, 0.9, 1.6, null.asInstanceOf[Double], 0.9, 1.0) zip
      List[String]("A", "A", "A", "B", "A", "B", "A", "A", "A", "B", "A", "B", "B", "B") toList

    val q = sparkSession.createDataFrame(
      sc.parallelize(
        data3.map { x => Row(x._1, x._2) }),
      StructType(List(
        StructField("value", DoubleType, nullable = true),
        StructField("type", StringType, nullable = true))))

    q.show()

    import org.apache.spark.sql.functions.{array, callUDF, lit}

    // define UDAF
    val myPercentile = new BenHaimTomApproximatePercentileV2(List(30, 70), 100, 0.01)

    // Compare results between the HIVE UDF and the Custom UDF
    val result = q.groupBy("type").agg(myPercentile(col("value")).as("percentiles"))
    result.show()

    val result2 = q.groupBy("type").agg(
      callUDF("percentile_approx", col("value"), array(List(0.3, 0.7).map { x => lit(x) }: _*)))
    result2.show()

    def roundAt: Double => Double = x => (x * 10).round.toDouble / 10

  }
}