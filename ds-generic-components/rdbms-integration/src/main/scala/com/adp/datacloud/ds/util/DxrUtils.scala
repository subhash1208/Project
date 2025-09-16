package com.adp.datacloud.ds.util

import org.apache.spark.{HashPartitioner, Partitioner}

// common utils for modules using DXR
object DxrUtils {
  // do not put any code that is not serializable or can't used in udfs

  def getDbSchema(ownerID: String, databaseDNS: String, databaseUser: String): String = {
    val serviceNameRegex = s"(?s)(?i).*service_name=(\\w+).*".r
    val serviceName =
      if (databaseDNS.matches(serviceNameRegex.regex)) {
        val serviceNameRegex(serviceNameMatch) = databaseDNS
        serviceNameMatch
      } else {
        s""
      }
    List(ownerID.trim, serviceName.trim, databaseUser.trim).mkString("|")
  }

}

case class DXRConnection(
    SOURCE_DB: String,
    DB_USER_NAME: String,
    DB_PASSWORD: String,
    TARGET_KEY: String,
    OWNER_ID: String,
    DB_SCHEMA: String,
    JDBC_URL: String,
    DB_ID: Int = 0,
    SQL_NAME: String,
    FINAL_SQL: String,
    FINAL_SESSION_SETUP_CALL: String,
    TRANSACTION_ISOLATION_LVL: Int = 0,
    CLIENT_IDENTIFIER: String,
    ROW_ID: Long          = 0,
    BATCH_NUMBER: Long    = 0,
    PARTITION_KEY: String = "",
    LOWER_BOUND: Long     = 0,
    UPPER_BOUND: Long     = 200)

// TODO: Change variable names appropriately and define the assumptions and behavior of this class
@deprecated(message = "use RangePartitioner", since = "20.7.0")
class DXRCustomPartitioner(subpartitions: Int, numParts: Int) extends Partitioner {
  override def numPartitions: Int = subpartitions * numParts
  override def hashCode: Int      = numPartitions

  override def getPartition(key: Any): Int = {
    val token = key.asInstanceOf[String].split(s"_")
    val partition_id = token(0).toInt * subpartitions + (token
      .hashCode() % subpartitions).abs
    partition_id
  }

  override def equals(other: Any): Boolean =
    other match {
      case h: HashPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }
}
