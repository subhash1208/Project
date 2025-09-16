package org.apache.spark.com.adp.datacloud.ds

import com.adp.datacloud.ds.udafs.WrappedArrayBuffer
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

abstract class WrappedBufferUDT[T: ClassTag](val dataType: DataType) extends UserDefinedType[WrappedArrayBuffer[T]] {

  // Underlying storage type for the UDT
  // Accounts for the entire mutable state contained in the UDT
  def sqlType: DataType = StructType(Seq(
    StructField("capacity", IntegerType, nullable = false),
    StructField("currentSize", IntegerType, nullable = false),
    StructField("array", ArrayType(dataType, containsNull = false), nullable = true)))

  override def serialize(obj: WrappedArrayBuffer[T]): Any = obj match {
    case c: WrappedArrayBuffer[_] => {
      val row = new GenericInternalRow(3)
      row.setInt(0, c.capacity)
      row.setInt(1, c.currentSize)
      row.update(2, new GenericArrayData(c.underlyingAny))
      row
    }
    case other => throw new UnsupportedOperationException(s"Cannot serialize object ${other}")
  }

  def userClass: Class[WrappedArrayBuffer[T]] = classOf[WrappedArrayBuffer[T]]
}
