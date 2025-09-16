package com.adp.datacloud.ds

import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.types._

class UniqueValRatio extends UserDefinedAggregateFunction{
  
  override def inputSchema : StructType = StructType(StructField("value", StringType) :: Nil)
  override def dataType: DataType = DoubleType
  override def deterministic: Boolean = true
  
  override def bufferSchema: StructType = StructType(StructField("values", ArrayType(StringType)) :: Nil )
  
  override def initialize (buffer : MutableAggregationBuffer): Unit = {
    buffer(0) = new Array[String] (100000) }  
  
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0) ++  Array(input.getAs[String](0)) }
 
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[scala.collection.mutable.WrappedArray[String]](0) ++ buffer2.getAs[scala.collection.mutable.WrappedArray[String]](0) }

  override def evaluate(buffer: Row): Double = {
    val all = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0)
                    .filter(x => x.isInstanceOf[Any])
    val distincts = all.toSet
                       .toList
   
    val uniques = distincts.map(m => all.filter(x => x.equals(m)).size.toDouble)
                         .filter(a => a == 1.0)
                         .size
                         .toDouble
     
    uniques/(distincts.size.toDouble)
  }
}
