package com.adp.datacloud.ds

case class DimensionSummary (dimensionSpec: String,
      dataTypes: String,
      cardinality: Long
    ) {
  
  override def toString = "(" + dimensionSpec + ", " + dataTypes + ", " + cardinality + ")"
  
}

case class CubeDimensionSummary(listOfWarnings: Seq[String],
    dimensionSummary: Seq[DimensionSummary]) {
    
  override def toString = "[" + listOfWarnings.mkString(",\n\t   ") + "], \n\t[" + dimensionSummary.mkString(",\n\t   ") + "]"

}