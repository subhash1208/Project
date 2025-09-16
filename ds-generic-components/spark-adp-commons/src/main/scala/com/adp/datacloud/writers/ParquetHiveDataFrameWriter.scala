package com.adp.datacloud.writers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

/**
 * Writer class to persist a dataframe to hive in parquet format
 */
class ParquetHiveDataFrameWriter private[writers] (
  partitionsString:         String              = "",
  partitionColumns:         List[String]        = List(),
  staticPartitionMap:       Map[String, String] = Map(),
  addonDistributionColumns: List[String]= List())(implicit sparkSession: SparkSession)
  extends HiveDataFrameWriter(
    partitionsString,
    partitionColumns,
    staticPartitionMap,
    addonDistributionColumns) {

  override def sparkFormatSpecifier = s" USING parquet"

  override def hiveFormatSpecifier = s" STORED AS parquet"

  override def hiveTableProperties =
    s"TBLPROPERTIES ('parquet.compression'='SNAPPY')"

  override def getCreateTableDDL(
    tableName: String,
    fields:    Array[(String, String)],
    location:  Option[String]): String = {
    getCreateSparkTableDDL(tableName, fields, location)
  }

  override def getColumnDefinitions(
    schema: StructType): Array[(String, String)] = {
    // TODO: check if using direct dataframe schema has any issue
    // faced errors while case stmts were present in select clause
    //    schema.fields.map(x => x.name.toLowerCase -> x.dataType.catalogString)
    schema.fields.map { x =>
      x.name.toLowerCase -> toHiveMetastoreType(x.dataType)
    }
  }
}
