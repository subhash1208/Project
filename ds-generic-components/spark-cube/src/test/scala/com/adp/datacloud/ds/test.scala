package com.adp.datacloud.ds

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.spark.com.adp.datacloud.ds.BufferTypeTest
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

object test {

  type Buffer = ArrayBuffer[Long]

  case object BufferType extends BufferTypeTest

  class TimeCollectionFunction(private val tslimit: Int)
      extends UserDefinedAggregateFunction {
    def inputSchema: StructType =
      StructType(StructField("value", LongType, false) :: Nil)

    def bufferSchema: StructType =
      StructType(StructField("list", BufferType, true) :: Nil)

    override def dataType: DataType = BufferType

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = new ArrayBuffer[Long]()
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (buffer != null && !input.isNullAt(0)) {
        var buf = buffer(0).asInstanceOf[ArrayBuffer[Long]]
        if (buf.length < tslimit) {
          buf.append(input.getAs[Long](0))
          buffer(0) = buf
        } else {
          buffer(0) = null
          buf       = null
        }
      }
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      if (buffer1 != null && buffer2(0) != null) {
        var buf1 = buffer1(0).asInstanceOf[ArrayBuffer[Long]]
        var buf2 = buffer2(0).asInstanceOf[ArrayBuffer[Long]]
        if (buf1.length + buf2.length <= tslimit) {
          buf1.appendAll(buf2)
          buffer1(0) = buf1
        } else {
          buffer1(0) = null
          buf1       = null
          buf2       = null
        }
      }
    }

    def evaluate(buffer: Row): Any = {
      if (buffer(0) == null) {
        new ArrayBuffer[Long]()
      } else {
        buffer(0).asInstanceOf[ArrayBuffer[Long]]
      }
    }
  }

  def main(args: Array[String]) {
    val mylist = List(
      "CLNT_OBJ_ID",
      "D_FULL_TM_PART_TM_CD",
      "D_PAY_RT_TYPE_CD",
      "D_JOB_CD",
      "D_WORK_LOC_CD",
      "D_CMPNY_CD",
      "D_HR_ORGN_ID",
      "D_GNDR_CD",
      "D_EEO_ETHNCTY_CLSFN_DSC",
      "D_MARTL_STUS_CD",
      "D_REG_TEMP_DSC",
      "D_EEO1_JOB_CATG_CD",
      "D_FLSA_STUS_DSC",
      "EEO1_JOB_CATG_DSC",
      "D_ONET_CODE",
      "WORK_LOC_CD",
      "D_WORK_STATE_CD")

    val baseGroupingSets = (1 to 3).flatMap { x => mylist.combinations(x) } filter { y =>
      y.contains("CLNT_OBJ_ID")
    } toList
    val all = List("QTR_CD", "MNTH_CD", "YR_CD").flatMap { x =>
      baseGroupingSets.map { y => y ++ List(x) }
    }
    println(all mkString ",\n")
    println(mylist.mkString(","))

    implicit val sparkSession = SparkSession.builder
      .master("local")
      .appName("CubeTesting")
      .enableHiveSupport()
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    val timeParquetStream = Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream("t_dim_day/t_dim_day.parquet")

    val filePath = "/tmp/t_dim_day/t_dim_day.parquet"
    val tfile    = new File(filePath)
    val parent   = tfile.getParentFile();
    if (parent != null) {
      Files.createDirectories(parent.toPath());
      parent.setReadable(true, false)
      parent.setWritable(true, false)
      parent.setExecutable(true, false)
    }
    tfile.createNewFile()
    tfile.setReadable(true, false); // Needed to avoid permission errors
    tfile.setWritable(true, false); // Needed to avoid permission errors
    Files.copy(timeParquetStream, tfile.toPath(), StandardCopyOption.REPLACE_EXISTING)
    import org.apache.spark.sql.functions.{count, lit}
    sparkSession.read
      .parquet("/tmp/t_dim_day")
      .cube("yr_cd", "qtr_cd")
      .agg(count(lit(1)).as("calendar_days"))
      .filter("yr_cd > 2008")
      .show(100)

  }

  def main2(args: Array[String]) {

    implicit val sparkSession = SparkSession.builder
      .master("local")
      .appName("CubeTesting")
      .enableHiveSupport()
      .getOrCreate()

    lazy val sc = sparkSession.sparkContext

    val schema = StructType(StructField("list", BufferType, false) :: Nil)

    val rdd = sc
      .parallelize(0 to 10)
      .map(x => {
        val seq = new Buffer()
        seq.appendAll(Seq(1, 2, 3, 4, 5))
        Row(seq)
      })

    val df = sparkSession.createDataFrame(rdd, schema)

    //df.show();

    import sparkSession.implicits._
    val a = sc.parallelize(0L to 20L).map(x => (x, x % 4)).toDF("value", "group")

    a.show();
    val cl = new TimeCollectionFunction(5)
    a.groupBy("group").agg(cl($"value").as("list")).show()

  }

}
