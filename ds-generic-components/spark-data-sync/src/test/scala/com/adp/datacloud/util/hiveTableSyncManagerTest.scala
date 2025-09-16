package com.adp.datacloud.util

import java.util.{Calendar, Date}

import com.adp.datacloud.cli.{HiveTableSyncManagerConfig, hiveTableSyncManagerOptions}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class EMP(Name: String, Age: Int, Designation: String, Salary: Int, Ooid: String)

object hiveTableSyncManagerTest extends  SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  val adpInternalClient = s"56792"

  def main(args: Array[String]) {
    cleanupDbs
    createDbsAndData
    testCopyFullTable
    val config = hiveTableSyncManagerOptionsParser
    runSyncByConfig(config)
    testForceSync
    testCopyByPartition
    testSchemaChange
    testSkipSync
    testDDLGeneration

  }

  def hiveTableSyncManagerOptionsParser() = {

    val args =
      s"""--ddl-replacements  cdldsraw2=cdldsraw3,prd-ds-live=fit-ds-live,942420684378=591853665740
         --jdbc-url jdbc:oracle:thin:@cdldftef1-scan.es.ad.adp.com:1521/cri03q_svc1
         --parallel-hive-writes 3 --catalog-table-name none
         --sync-schema-pairs cdldsraw1:cdldsraw2
         --s3-bucket-name datacloud-prod-dump-us-east-1-708035784431
         --ddl-file-name cdldsraw.sql
         --ddl-only false""".trim().split("\\s+")

    println(args.toList)
    val config = hiveTableSyncManagerOptions.parse(args)

    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val configJSON: String                    = write(config)
    println(configJSON)
    config
  }

  def cleanupDbs(implicit sparkSession: SparkSession) {
    sparkSession.sql("""DROP DATABASE IF EXISTS cdldsraw1 CASCADE""")
    sparkSession.sql("""DROP DATABASE IF EXISTS cdldsraw2 CASCADE""")
  }

  def createDbsAndData(implicit sparkSession: SparkSession) {
    import sparkSession.implicits._

    val dblist = sparkSession.sql("show databases")
    dblist.show

    sparkSession.sql(
      """CREATE DATABASE IF NOT EXISTS cdldsraw1 COMMENT "Testdb" LOCATION "/tmp/spark-warehouse/cdldsraw1.db" """)
    sparkSession.sql(
      """CREATE DATABASE IF NOT EXISTS cdldsraw2 COMMENT "Testdb" LOCATION "/tmp/spark-warehouse/cdldsraw2.db" """)

    val emp1 = EMP("Solid Snake", 21, "MT", 2000, "56798")
    val emp2 = EMP("Big Boss", 62, "MT", 2000, "56797")
    val emp3 = EMP("Revolver Ocelot", 23, "MT", 2000, "56796")
    val emp4 = EMP("Liquid Snake", 24, "SMT", 3000, "56795")
    val emp5 = EMP("EVA", 25, "SMT", 3000, "56794")
    val emp6 = EMP("Raiden", 26, "SMT", 4000, "56793")
    val emp7 = EMP("Venom Snake", 27, "SMT", 4000, "56792")
    val emp8 = EMP("Skull Face", 37, "SMT", 4000, null)
    val emp9 = EMP("Quite", 24, "SMT", 4000, "56792")

    val empDf = List(emp1, emp2, emp3, emp4, emp5, emp6, emp7, emp8, emp9).toDF
    empDf.show()

    empDf.write.saveAsTable("cdldsraw1.emp_data_plain")
    empDf.write.partitionBy("Designation", "Salary").saveAsTable("cdldsraw1.emp_data")
  }

  def runSyncByConfig(config: HiveTableSyncManagerConfig)(implicit
      sparkSession: SparkSession) {
    hiveTableSyncManager.buildAndRunSync(config)
  }

  def testCopyFullTable(implicit sparkSession: SparkSession) {
    val cRow1           = SyncUnit("emp_data", "cdldsraw1", "cdldsraw2")
    val cRow2           = SyncUnit("emp_data_plain", "cdldsraw1", "cdldsraw2")
    val catalogRowsList = List(cRow1, cRow2)
    hiveTableSyncManager.syncTables(catalogRowsList, adpInternalClient)
    sparkSession.table("cdldsraw2.emp_data").show
  }

  def testForceSync(implicit sparkSession: SparkSession) {
    val cal = Calendar.getInstance()
    cal.setTime(new Date)
    val cRow =
      SyncUnit("emp_data_plain", "cdldsraw1", "cdldsraw2", cal.get(Calendar.DAY_OF_MONTH))
    val catalogRowsList = List(cRow)
    hiveTableSyncManager.syncTables(catalogRowsList, adpInternalClient)
  }

  def testCopyByPartition(implicit sparkSession: SparkSession) {
    sparkSession.sql(
      "ALTER TABLE cdldsraw2.emp_data DROP PARTITION (designation='MT',salary='2000'),PARTITION (designation='SMT',salary='4000')")
    val cRow            = SyncUnit("emp_data", "cdldsraw1", "cdldsraw2")
    val catalogRowsList = List(cRow)
    hiveTableSyncManager.syncTables(catalogRowsList, adpInternalClient)
  }

  def testSchemaChange(implicit sparkSession: SparkSession) {
    // spark does not allow column modification through sql hence recreating the table to simulate the senario
    sparkSession.sql("DROP TABLE IF EXISTS cdldsraw2.emp_data")
    sparkSession.sql(
      "CREATE TABLE cdldsraw2.emp_data AS SELECT Name,Age,Ooid FROM cdldsraw1.emp_data")
    val cRow            = SyncUnit("emp_data", "cdldsraw1", "cdldsraw2")
    val catalogRowsList = List(cRow)
    hiveTableSyncManager.syncTables(catalogRowsList, adpInternalClient)
  }

  def testSkipSync(implicit sparkSession: SparkSession) {
    // this step is to test if the tables already synced and skip copy
    val cRow1           = SyncUnit("emp_data", "cdldsraw1", "cdldsraw2")
    val cRow2           = SyncUnit("emp_data_plain", "cdldsraw1", "cdldsraw2")
    val catalogRowsList = List(cRow1, cRow2)
    hiveTableSyncManager.syncTables(catalogRowsList, adpInternalClient)
  }

  def testDDLGeneration(implicit sparkSession: SparkSession) {
    System.setProperty("http.proxyHost", "localhost");
    System.setProperty("http.proxyPort", "3128");
    System.setProperty("proxySet", "true");
    val cRow1           = SyncUnit("emp_data", "cdldsraw1", "cdldsraw2")
    val cRow2           = SyncUnit("emp_data_plain", "cdldsraw1", "cdldsraw2")
    val catalogRowsList = List(cRow1, cRow2)
    hiveTableSyncManager.extractDDLs(
      catalogRowsList,
      "adp-ds-prod-mr",
      "syncTestDDL.sql",
      Map())
  }

}
