package com.adp.datacloud.cli

case class ExtractorTaskLog(
  APPLICATION_ID:     String,
  ATTEMPT_ID:         Option[String],
  TARGET_KEY:         String,
  PARTITION:          Option[String],
  DB_USER_NAME:       String,
  JDBC_URL:           String,
  OWNER_ID:           String,
  CLIENT_IDENTIFIER:  String,
  SESSION_SETUP_CALL: String,
  MSG:                String,
  IS_ERROR:           Boolean        = false)

abstract class DataExtractorConfig {
  val inputParams: Map[String, String]
  val maxParallelThreads: Integer
  val batchColumn: Option[String]
  val batchColumnDimension: Option[String]
  val upperBound: Long
  val numPartitions: Integer
  val rdbmsFetchSize: Integer
  val saveFormat: String
  val outputPartitionSpec: Option[String]
  val outputDatabaseName: String
  val blueDatabaseName: Option[String]
  val outputDatabaseTable: String
  val overwriteOutputTable: Boolean
  val replaceStringLiterals: Option[List[String]]
  val sql: String
  val dxrJdbcURL: Option[String] = None
  def sqlBatched(inputSql: String): String = {
    "(with batches as (select /*+ materialize */ " +
      batchColumn.get + ", ntile(" + upperBound + ") over (order by " + batchColumn.get + " ) as batch_number " +
      "from " + batchColumnDimension.get + ") " +
      "select b1.batch_number, main.* " +
      "from batches b1 " +
      "inner join " + inputSql + " main " +
      "on main." + batchColumn.get + " = b1." + batchColumn.get + ")"
  }
}
