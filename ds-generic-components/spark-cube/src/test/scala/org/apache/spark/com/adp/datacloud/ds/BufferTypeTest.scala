package org.apache.spark.com.adp.datacloud.ds

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, UserDefinedType}

import scala.collection.mutable.ArrayBuffer

class BufferTypeTest extends UserDefinedType[ArrayBuffer[Long]] {

  type Buffer = ArrayBuffer[Long]
  def sqlType: DataType = ArrayType(LongType, false)

  def serialize(obj: ArrayBuffer[Long]): Any =
    obj match {
      case c: Buffer => new GenericArrayData(c.toArray) // c.toSeq
      case other =>
        throw new UnsupportedOperationException(s"Cannot serialize object ${other}")
    }

  /** Convert a SQL datum to the user type */
  def deserialize(datum: Any): Buffer =
    datum match {
      case a: ArrayData =>
        a.toArray(LongType)
          .toBuffer
          .asInstanceOf[Buffer] //a.toBuffer.asInstanceOf[Buffer]
      case other =>
        throw new UnsupportedOperationException(s"Cannot deserialize object ${other}")
    }

  def userClass: Class[Buffer] = {
    classOf[Buffer]
  }

  override def defaultSize: Int = 1500
}
