package com.adp.datacloud.ds

import org.apache.spark.sql.functions.{ udf }

object dixonsQValueUDF extends Serializable {
  def dixonsQValue(input:Seq[Double]) : Double = {
  try{ 
    val data = input.toArray.reverse
    val expValue=data(0)
    val qExp = if ((data.max - data.min) != 0) {
      (data.drop(1).map((expValue-_)).map(math.abs(_)).min)/(data.max - data.min)
    } else 0.00
    qExp
  }
  catch {
    case e: Exception => 0.00}
  }
  val getQValue =  udf[Double, Seq[Double]](dixonsQValue)
}