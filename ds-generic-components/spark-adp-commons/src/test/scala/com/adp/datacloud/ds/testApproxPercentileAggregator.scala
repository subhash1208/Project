package com.adp.datacloud.ds

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Row, SparkSession}

import scala.annotation.tailrec
import scala.collection.mutable.WrappedArray

//import testUdafs._

case class Histogram(keys: WrappedArray[Double], values: WrappedArray[Long])

case class DoubleArray(keys: WrappedArray[Double])

object testApproxPercentileAggregator {

  /**
   * This method returns a tuple with two options (index, lastIndexLessThanKey)
   * If the key is found in the histogram then index will the index of the key in the histogram
   * If the key is not found in the histogram, then lastIndexLessThanKey holds the greatest index of the histogram where the value is greater than the key
   */
  @tailrec
  private final def getNearestIndices(
      histogram: WrappedArray[Double],
      key: Double,
      startIndex: Int,
      endIndex: Int): (Option[Int], Option[Int]) = {

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
      histogramKeys: WrappedArray[Double],
      histogramValues: WrappedArray[Long],
      maxHistogramSize: Int,
      key: Double,
      value: Long = 1): (WrappedArray[Double], WrappedArray[Long]) = {

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
          val keysTuple   = histogramKeys.splitAt(splitIndex + 1)
          val valuesTuple = histogramValues.splitAt(splitIndex + 1)
          (
            keysTuple._1 ++ Array(key) ++ keysTuple._2,
            valuesTuple._1 ++ Array(value) ++ valuesTuple._2)
        } else {
          // This histogram size is already full. Find the closest two elements and merge them.
          // This code is intentionally written in an imperative fashion for efficiency reasons
          var i                              = 1;
          var closestElementIndex            = 0;
          var closestElementIndexForNewInput = 0;
          var closestDistance                = histogramKeys(1) - histogramKeys(0)
          var closestDistanceForNewInput     = key - histogramKeys(0)
          while (i < maxHistogramSize) {
            if (histogramKeys(i) - histogramKeys(i - 1) < closestDistance) {
              closestDistance     = histogramKeys(i) - histogramKeys(i - 1)
              closestElementIndex = i - 1
            }
            if (math.abs(key - histogramKeys(i)) < closestDistanceForNewInput) {
              closestDistanceForNewInput     = math.abs(key - histogramKeys(i))
              closestElementIndexForNewInput = i
            }
            i += 1
          }

          if (closestDistance >= closestDistanceForNewInput) {
            // Simpler case. Merge the new input with the closest element to it
            histogramKeys(closestElementIndexForNewInput) =
              (histogramKeys(closestElementIndexForNewInput) * histogramValues(
                closestElementIndexForNewInput) +
                (key * value)) / (histogramValues(closestElementIndexForNewInput) + value)
            histogramValues(closestElementIndexForNewInput) =
              histogramValues(closestElementIndexForNewInput) + value
          } else {
            // Two of the existing histogram elements should be merged and the new input should be added to the list
            if (closestElementIndexForNewInput > closestElementIndex) {
              // The new element to be added is to the right of the pair of closest elements
              histogramKeys(closestElementIndex) = (histogramKeys(
                closestElementIndex) * histogramValues(closestElementIndex) +
                histogramKeys(closestElementIndex + 1) * histogramValues(
                  closestElementIndex + 1)) / (histogramValues(
                closestElementIndex) + histogramValues(closestElementIndex + 1))
              histogramValues(closestElementIndex) = histogramValues(
                closestElementIndex) + histogramValues(closestElementIndex + 1)
              var i = closestElementIndex + 1
              while (histogramKeys(i) < key && i < maxHistogramSize - 1) {
                histogramKeys(i)   = histogramKeys(i + 1)
                histogramValues(i) = histogramValues(i + 1)
                i += 1
              }
              histogramKeys(i)   = key
              histogramValues(i) = value
            } else {
              // The new element to be added is to the left of the pair of closest elements
              histogramKeys(closestElementIndex + 1) = (histogramKeys(
                closestElementIndex) * histogramValues(closestElementIndex) +
                histogramKeys(closestElementIndex + 1) * histogramValues(
                  closestElementIndex + 1)) / (histogramValues(
                closestElementIndex) + histogramValues(closestElementIndex + 1))
              histogramValues(closestElementIndex + 1) = histogramValues(
                closestElementIndex) + histogramValues(closestElementIndex + 1)
              var i = closestElementIndex
              while (histogramKeys(i) > key && i > 0) {
                histogramKeys(i)   = histogramKeys(i - 1)
                histogramValues(i) = histogramValues(i - 1)
                i -= 1
              }
              histogramKeys(i)   = key
              histogramValues(i) = value
            }
          }

          (histogramKeys, histogramValues)

        }

        // End

      }
    }

  }

  def main(args: Array[String]) {

    lazy val sparkSession = SparkSession.builder
      .master("local")
      .appName("PercentileAggregatorTest")
      .enableHiveSupport()
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    import sparkSession.implicits._

    val simpleSum = new Aggregator[Int, Int, Int] with Serializable {
      def zero: Int               = 0 // The initial value.
      def reduce(b: Int, a: Int)  = b + a // Add an element to the running total
      def merge(b1: Int, b2: Int) = b1 + b2 // Merge intermediate values.
      def finish(b: Int)          = b // Return the final result.

      def bufferEncoder: Encoder[Int] = ExpressionEncoder()
      def outputEncoder: Encoder[Int] = ExpressionEncoder()

    }.toColumn

    val ds = Seq(1, 2, 3, 4).toDS()
    ds.show()
    ds.select(simpleSum).show()

    val percentile = new Aggregator[Double, Histogram, DoubleArray] with Serializable {

      // The initial value.
      def zero = Histogram(Array[Double](), Array[Long]())

      // Add an element to the running total
      def reduce(b: Histogram, a: Double) = {
        val updatedHistogramTuple = add(b.keys, b.values, 10000, a, 1)
        Histogram(updatedHistogramTuple._1, updatedHistogramTuple._2)
      }

      // Merge intermediate values.
      def merge(b1: Histogram, b2: Histogram) = {

        // Merge the two histograms
        val mergedHistogramTuple =
          (b2.keys zip b2.values).foldRight((b1.keys, b1.values)) {
            (currentMergeTuple, histogramAccumulator) =>
              {
                add(
                  histogramAccumulator._1,
                  histogramAccumulator._2,
                  10000,
                  currentMergeTuple._1,
                  currentMergeTuple._2)
              }
          }

        Histogram(mergedHistogramTuple._1, mergedHistogramTuple._2)

      }

      // Return the final result.
      def finish(b: Histogram) = {

        val percentiles = List(25, 50, 70)

        if (b.keys.isEmpty) {
          DoubleArray(Array.fill(percentiles.size)(Double.NaN))
        } else {

          val numElements = b.values.reduce(_ + _)
          val result = percentiles
            .map({ x =>
              {

                @tailrec
                def getKeyForNthValue(
                    histogramKeys: WrappedArray[Double],
                    histogramValues: WrappedArray[Long],
                    nthValue: Double,
                    prevBinValue: Double = 0): Double = {

                  if (histogramKeys.isEmpty) {
                    // would never happen
                    Double.NaN
                  } else {
                    if (histogramValues.head > nthValue) {
                      prevBinValue + ((histogramKeys.head - prevBinValue) * nthValue / histogramValues.head)
                    } else if (histogramValues.head == nthValue) {
                      histogramKeys.head
                    } else
                      getKeyForNthValue(
                        histogramKeys.tail,
                        histogramValues.tail,
                        nthValue - histogramValues.head,
                        histogramKeys.head)
                  }
                }

                getKeyForNthValue(b.keys, b.values, x * numElements / 100d)

            }
          }) toArray

          DoubleArray(result)

        }

      }

      def bufferEncoder = ExpressionEncoder()
      def outputEncoder = ExpressionEncoder()

    }.toColumn

    val data = List[Double](
      10,
      29,
      15,
      43,
      9,
      12,
      null.asInstanceOf[Double],
      13,
      16,
      9,
      16,
      null.asInstanceOf[Double],
      9,
      10) zip
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
        "B") toList

    val data2 = List(66.66666666666666, 75.0, 80.0, 0.0, 33.33333333333333, 100.0, 0,
      33.33333333333333, 50) zip
      List[String]("B", null, "A", "B", null, "A", "B", null, "A") toList

    val q = sparkSession.createDataFrame(
      sc.parallelize(data.map { x => Row(x._1, x._2) }),
      StructType(
        List(
          StructField("value", DoubleType, nullable = true),
          StructField("type", StringType, nullable  = true))))

    q.show()

    import org.apache.spark.sql.functions.{array, callUDF, lit}

    val doubleSum = new Aggregator[Double, Double, Double] with Serializable {
      def zero: Double                   = 0 // The initial value.
      def reduce(b: Double, a: Double)   = b + a // Add an element to the running total
      def merge(b1: Double, b2: Double)  = b1 + b2 // Merge intermediate values.
      def finish(b: Double)              = b // Return the final result.
      def bufferEncoder: Encoder[Double] = ExpressionEncoder()
      def outputEncoder: Encoder[Double] = ExpressionEncoder()

    }.toColumn

    class MyAgg[I](f: I => Any) extends Aggregator[I, Double, Double] with Serializable {
      def zero: Double = 0 // The initial value.
      def reduce(b: Double, a: I) =
        b + f(a).asInstanceOf[Double] // Add an element to the running total
      def merge(b1: Double, b2: Double) = b1 + b2 // Merge intermediate values.
      def finish(b: Double)             = b // Return the final result.

      def bufferEncoder = ExpressionEncoder()
      def outputEncoder = ExpressionEncoder()

    }

    class BelowThreshold[I](f: I => Boolean)
        extends Aggregator[I, Boolean, Boolean]
        with Serializable {
      def zero                                = false
      def reduce(acc: Boolean, x: I)          = acc | f(x)
      def merge(acc1: Boolean, acc2: Boolean) = acc1 | acc2
      def finish(acc: Boolean)                = acc

      def bufferEncoder = ExpressionEncoder()
      def outputEncoder = ExpressionEncoder()
    }

    val belowThreshold = new BelowThreshold[(Double, String)](_._1 < 10).toColumn

    val myAgg = new MyAgg[Tuple2[Double, String]](_._1).toColumn

    // Compare results between the HIVE UDF and the Custom UDF
    //val result = q.groupBy("type").agg(percentile(col("value")).as("percentiles"))
    //val result = q.groupBy("type").agg(doubleSum(col("value")).as("percentiles"))
    //result.show()

    //    q.as[(Double, String)].groupBy(_._2).count().show() //agg(myAgg).show()
    //    q.as[(Double, String)].groupBy(_._2).agg(belowThreshold).show() //agg(myAgg).show()

    val result2 = q
      .groupBy("type")
      .agg(
        callUDF(
          "percentile_approx",
          col("value"),
          array(List(0.3, 0.7).map { x => lit(x) }: _*)))
    result2.show()

  }

}
