package com.adp.datacloud.ds

import org.apache.spark.com.adp.datacloud.ds.WrappedBufferUDT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DoubleType, LongType, SQLUserDefinedType}

package object udafs {

  @SQLUserDefinedType(udt = classOf[DoubleBufferType])
  type DoubleBuffer = WrappedArrayBuffer[Double]

  @SQLUserDefinedType(udt = classOf[LongBufferType])
  type LongBuffer = WrappedArrayBuffer[Long]

  class DoubleBufferType extends WrappedBufferUDT[Double](DoubleType) {

    override def deserialize(datum: Any): WrappedArrayBuffer[Double] = datum match {
      case row: InternalRow =>
        require(
          row.numFields == 3,
          s"WrappedBufferUDT.deserialize given row with length ${row.numFields} but requires length == 3")
        val capacity = row.getInt(0)
        val currentSize = row.getInt(1)
        val array = row.getArray(2).toArray[Double](DoubleType)
        WrappedArrayBuffer(array, capacity, currentSize)
    }

  }

  class LongBufferType extends WrappedBufferUDT[Long](LongType) {

    override def deserialize(datum: Any): WrappedArrayBuffer[Long] = datum match {
      case row: InternalRow =>
        require(
          row.numFields == 3,
          s"WrappedBufferUDT.deserialize given row with length ${row.numFields} but requires length == 3")
        val capacity = row.getInt(0)
        val currentSize = row.getInt(1)
        val array = row.getArray(2).toArray[Long](LongType)
        WrappedArrayBuffer(array, capacity, currentSize)
    }

  }

  case object DoubleBufferType extends DoubleBufferType
  case object LongBufferType extends LongBufferType

}