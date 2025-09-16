package com.adp.datacloud.ds

import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ col, udf }

class EntropyUdf extends UserDefinedAggregateFunction{
  
  override def inputSchema : StructType = StructType(StructField("value", StringType) :: Nil)
  override def dataType: DataType = DoubleType
  override def deterministic: Boolean = true
  
  override def bufferSchema: StructType = StructType(StructField("values", ArrayType(StringType)) :: Nil )
  
  override def initialize (buffer : MutableAggregationBuffer): Unit = {
    buffer(0) = new Array[String] (100000) }  
  
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0) ++  Array(input.getAs[String](0))   }
 
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[scala.collection.mutable.WrappedArray[String]](0) ++ buffer2.getAs[scala.collection.mutable.WrappedArray[String]](0) }

  override def evaluate(buffer: Row): Double = {
    val raw = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0)
                    
    val all = raw.filter(_.isInstanceOf[Any])
                 .map(_.trim())
                 .filter(_.isInstanceOf[Any])
                    
    val uniques = all.toSet
                     .toList
    val numrows = all.size
                     .toDouble
    val freqs = uniques.map(m => all.filter(x => x.equals(m)).size.toDouble)
    val entropy = freqs.map(x => {
                     if (x != 0.0 & numrows != 0.0)
                       -(x / numrows) * math.log(x/numrows)
                     else
                       0.0 })
         .reduceOption(_+_)
   
    entropy match {
      case Some(x) => x
      case None    => 0.0  
    }
  } 
}