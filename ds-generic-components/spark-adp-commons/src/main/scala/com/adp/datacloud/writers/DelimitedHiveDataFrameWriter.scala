package com.adp.datacloud.writers

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types.StructType

/**
 * Writer class to persist a dataframe to hive in delimited format.
 *
 * Lots of extensions of features possible in these writer classes (including abstract writer) to expand tables when new columns are added etc..
 *
 */
class DelimitedHiveDataFrameWriter private[writers] (
  separator:                Char,
  partitionsString:         String              = "",
  partitionColumns:         List[String]        = List(),
  staticPartitionMap:       Map[String, String] = Map(),
  addonDistributionColumns: List[String]        = List())(implicit sparkSession: SparkSession)
  extends HiveDataFrameWriter(
    partitionsString,
    partitionColumns,
    staticPartitionMap,
    addonDistributionColumns) {

  override def sparkFormatSpecifier: String = " USING csv OPTIONS (delimiter '" + separator + "')"

  override def hiveFormatSpecifier: String = " row format delimited fields terminated by '" + separator + "' "
  
  override def getCreateTableDDL(tableName: String, fields: Array[(String, String)], location: Option[String]): String = {
    getCreateHiveTableDDL(tableName, fields, location)
  }

  override def getColumnDefinitions(schema: StructType): Array[(String, String)] = {
    schema.fields.map { x => x.name.toLowerCase -> toHiveMetastoreType(x.dataType) }
  }
}