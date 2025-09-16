package com.adp.datacloud.ds.udafs

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.annotation.tailrec
import scala.collection.mutable.WrappedArray

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
 *
 */
@Deprecated
class BenHaimTomApproximatePercentile(
  percentiles: Seq[Int],
  p:           Int      = 10000) extends UserDefinedAggregateFunction {

  require(p >= 10 && p <= 10000, "Parameter (p) must be between 10 and 10000")
  require(percentiles.forall { x => x > 0 && x < 100 }, "")

  def inputSchema: StructType = StructType(Array(
    StructField("value", DoubleType) // The column on which percentiles have to be calculated
  ))

  def deterministic = true

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("histogram_keys", ArrayType(DoubleType, true)),
    StructField("histogram_values", ArrayType(LongType, true)) // represents occurrence count
  ))

  // Returned Data Type .
  def dataType: DataType = ArrayType(DoubleType, true)

  // Ambitious but bad.
  private var myHistogramKeys = Array[Double]()
  private var myHistogramValues = Array[Long]()

  // Called whenever the key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = Array[Double]()
    buffer(1) = Array[Long]()
  }

  /**
   * This method returns a tuple with two options (index, lastIndexLessThanKey)
   * If the key is found in the histogram then index will the index of the key in the histogram
   * If the key is not found in the histogram, then lastIndexLessThanKey holds the greatest index of the histogram where the value is greater than the key
   */
  @tailrec
  private final def getNearestIndices(histogram: WrappedArray[Double], key: Double,
                                      startIndex: Int, endIndex: Int): (Option[Int], Option[Int]) = {

    if (histogram.size == 0 || startIndex < 0) {
      (None, None)
    } else if (startIndex > endIndex) {
      // Search finished. Key not found
      (None, Some(endIndex))
    } else {
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

  // Add element to the histogram
  private def add(
    histogramKeys:    WrappedArray[Double],
    histogramValues:  WrappedArray[Long],
    maxHistogramSize: Int,
    key:              Double,
    value:            Long                 = 1): (WrappedArray[Double], WrappedArray[Long]) = {

    val indices = getNearestIndices(histogramKeys, key, 0, histogramKeys.size - 1)
    indices._1 match {
      case Some(x) => {
        // Key found in the histogram
        histogramValues.update(x, histogramValues(x) + value)
        (histogramKeys, histogramValues)
      }
      case None => {
        // Key Not found in the histogram

        if (histogramKeys.size < maxHistogramSize) {
          val splitIndex = indices._2 match {
            case Some(y) => y
            case None    => -1
          }
          //val splitIndex = histogramKeys.lastIndexWhere { x => x < key }
          val keysTuple = histogramKeys.splitAt(splitIndex + 1)
          val valuesTuple = histogramValues.splitAt(splitIndex + 1)
          (keysTuple._1 ++ Array(key) ++ keysTuple._2, valuesTuple._1 ++ Array(value) ++ valuesTuple._2)
        } else {
          // This histogram size is already full. Find the closest two elements and merge them.
          // This code is intentionally written in an imperative fashion for efficiency reasons
          var i = 1;
          var closestElementIndex = 0;
          var closestElementIndexForNewInput = 0;
          var closestDistance = histogramKeys(1) - histogramKeys(0)
          var closestDistanceForNewInput = key - histogramKeys(0)
          while (i < maxHistogramSize) {
            if (histogramKeys(i) - histogramKeys(i - 1) < closestDistance) {
              closestDistance = histogramKeys(i) - histogramKeys(i - 1)
              closestElementIndex = i - 1
            }
            if (math.abs(key - histogramKeys(i)) < closestDistanceForNewInput) {
              closestDistanceForNewInput = math.abs(key - histogramKeys(i))
              closestElementIndexForNewInput = i
            }
            i += 1
          }

          if (closestDistance >= closestDistanceForNewInput) {
            // Simpler case. Merge the new input with the closest element to it
            histogramKeys(closestElementIndexForNewInput) = (histogramKeys(closestElementIndexForNewInput) * histogramValues(closestElementIndexForNewInput) +
              (key * value)) / (histogramValues(closestElementIndexForNewInput) + value)
            histogramValues(closestElementIndexForNewInput) = histogramValues(closestElementIndexForNewInput) + value
          } else {
            // Two of the existing histogram elements should be merged and the new input should be added to the list
            if (closestElementIndexForNewInput > closestElementIndex) {
              // The new element to be added is to the right of the pair of closest elements
              histogramKeys(closestElementIndex) = (histogramKeys(closestElementIndex) * histogramValues(closestElementIndex) +
                histogramKeys(closestElementIndex + 1) * histogramValues(closestElementIndex + 1)) / (histogramValues(closestElementIndex) + histogramValues(closestElementIndex + 1))
              histogramValues(closestElementIndex) = histogramValues(closestElementIndex) + histogramValues(closestElementIndex + 1)
              var i = closestElementIndex + 1
              while (histogramKeys(i) < key && i < maxHistogramSize - 1) {
                histogramKeys(i) = histogramKeys(i + 1)
                histogramValues(i) = histogramValues(i + 1)
                i += 1
              }
              histogramKeys(i) = key
              histogramValues(i) = value
            } else {
              // The new element to be added is to the left of the pair of closest elements
              histogramKeys(closestElementIndex + 1) = (histogramKeys(closestElementIndex) * histogramValues(closestElementIndex) +
                histogramKeys(closestElementIndex + 1) * histogramValues(closestElementIndex + 1)) / (histogramValues(closestElementIndex) + histogramValues(closestElementIndex + 1))
              histogramValues(closestElementIndex + 1) = histogramValues(closestElementIndex) + histogramValues(closestElementIndex + 1)
              var i = closestElementIndex
              while (histogramKeys(i) > key && i > 0) {
                histogramKeys(i) = histogramKeys(i - 1)
                histogramValues(i) = histogramValues(i - 1)
                i -= 1
              }
              histogramKeys(i) = key
              histogramValues(i) = value
            }
          }

          (histogramKeys, histogramValues)

        }

        // End

      }
    }

  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {

    if (input != null && !input.isNullAt(0) && !input.getDouble(0).isNaN()) {

      val histogramKeys = buffer.getSeq(0).asInstanceOf[WrappedArray[Double]]
      val histogramValues = buffer.getSeq(1).asInstanceOf[WrappedArray[Long]]
      val currentVal = input.getDouble(0)

      val updatedHistogramTuple = add(histogramKeys, histogramValues, p, currentVal)

      buffer(0) = updatedHistogramTuple._1
      buffer(1) = updatedHistogramTuple._2

    }

  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {

    val thisHistogramKeys = buffer1.getSeq(0).asInstanceOf[WrappedArray[Double]]
    val thisHistogramValues = buffer1.getSeq(1).asInstanceOf[WrappedArray[Long]]

    val thatHistogramKeys = buffer2.getSeq(0).asInstanceOf[WrappedArray[Double]]
    val thatHistogramValues = buffer2.getSeq(1).asInstanceOf[WrappedArray[Long]]

    // Merge the two histograms
    val mergedHistogramTuple = (thatHistogramKeys zip thatHistogramValues).foldRight((thisHistogramKeys, thisHistogramValues)) { (currentMergeTuple, histogramAccumulator) =>
      {
        add(histogramAccumulator._1, histogramAccumulator._2, p, currentMergeTuple._1, currentMergeTuple._2)
      }
    }

    buffer1(0) = mergedHistogramTuple._1
    buffer1(1) = mergedHistogramTuple._2

  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {

    val histogramKeys = Array[Double]() ++ buffer.getSeq(0).asInstanceOf[WrappedArray[Double]]
    val histogramValues = Array[Long]() ++ buffer.getSeq(1).asInstanceOf[WrappedArray[Long]]

    if (histogramKeys.isEmpty) {
      Array.fill(percentiles.size)(Double.NaN)
    } else {

      val numElements = histogramValues.reduce(_ + _)
      val result = percentiles.map({
        x =>
          {

            @tailrec
            def getKeyForNthValue(histogramKeys: Array[Double], histogramValues: Array[Long], nthValue: Double, prevBinValue: Double = 0): Double = {

              if (histogramKeys.isEmpty) {
                // would never happen
                Double.NaN
              } else {
                if (histogramValues.head > nthValue) {
                  prevBinValue + ((histogramKeys.head - prevBinValue) * nthValue / histogramValues.head)
                } else if (histogramValues.head == nthValue) {
                  histogramKeys.head
                } else
                  getKeyForNthValue(histogramKeys.tail, histogramValues.tail, nthValue - histogramValues.head, histogramKeys.head)
              }
            }

            getKeyForNthValue(histogramKeys, histogramValues, x * numElements / 100D)

          }
      }) toArray

      result

    }
    //new GenericArrayData(percentileValues)

  }

}

object benHaimTomApproximatePercentileTest {

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

    val q = sparkSession.createDataFrame(
      sc.parallelize(
        data.map { x => Row(x._1, x._2) }),
      StructType(List(
        StructField("value", DoubleType, nullable = true),
        StructField("type", StringType, nullable = true))))

    q.show()

    import org.apache.spark.sql.functions.{array, callUDF, lit}

    // define UDAF
    val myPercentile = new BenHaimTomApproximatePercentile(List(30, 70), 100)

    // Compare results between the HIVE UDF and the Custom UDF
    val result = q.groupBy("type").agg(myPercentile(col("value")).as("percentiles"))
    result.show()

    val result2 = q.groupBy("type").agg(
      callUDF("percentile_approx", col("value"), array(List(0.3, 0.7).map { x => lit(x) }: _*)))
    result2.show()

  }
}