package com.adp.datacloud.util

import com.adp.datacloud.cli.sparkDDLGeneratorOptions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object SparkDDLMigratorTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  val s3BuketName   = s"datacloud-prod-dump-us-east-1-708035784431"
  val sqlSuffix     = s"testgreenraw"
  val dbMigrateArgs = s"""--db-names testgreenraw --sql-file-prefix 708035784431
    --table-names ,
      --s3-bucket-name datacloud-prod-dump-us-east-1-708035784431
  		--ddl-replacements  cdldsraw2=cdldsraw3,prd-ds-live=fit-ds-live,942420684378=591853665740"""

  val tableMigrateArgs =
    s"""--table-names testgreenraw.emp_data_plain_hive,testgreenraw.emp_data_hive --sql-file-prefix 708035784431
    --db-names ,
      --s3-bucket-name datacloud-prod-dump-us-east-1-708035784431
  		--ddl-replacements  cdldsraw2=cdldsraw3,prd-ds-live=fit-ds-live,942420684378=591853665740"""

  def main(args: Array[String]) {
    val configForDbs    = testOptionsParser(dbMigrateArgs)
    val configForTables = testOptionsParser(tableMigrateArgs)
    setUpDbs
    createData
    val migrationsList = configForDbs.dbNames.flatMap(testMigrationDDlsByDb)
    testMigration(migrationsList)
    setUpDbs
    createData
    val migrationsTablesList = testMigrationDDlsByTableNames(configForTables.tableNames)
    //    testS3Upload(migrationsList,configForDbs.sqlPrefix,configForDbs.s3bucketName)
  }

  def setUpDbs(implicit sparkSession: SparkSession) {
    sparkSession.sql("""DROP DATABASE IF EXISTS testgreenraw CASCADE""")
    sparkSession.sql(
      """CREATE DATABASE IF NOT EXISTS testgreenraw COMMENT "Testdb" LOCATION "/tmp/spark-warehouse/testgreenraw.db" """)
  }

  def createData(implicit sparkSession: SparkSession) {
    import sparkSession.implicits._

    val dblist = sparkSession.sql("show databases")
    dblist.show

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

    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    empDf.write.saveAsTable("testgreenraw.emp_data_plain_spark")
    empDf.write
      .partitionBy("Designation", "Salary")
      .saveAsTable("testgreenraw.emp_data_spark")
    empDf.write
      .format("csv")
      .partitionBy("Designation", "Salary")
      .saveAsTable("testgreenraw.emp_data_csv_spark")
    sparkSession.sql(
      "CREATE TABLE testgreenraw.emp_data_plain_hive (Name STRING, Age INT, Designation STRING, Salary INT, Ooid STRING) STORED AS parquet")
    empDf.write.insertInto("testgreenraw.emp_data_plain_hive")
    // partition columns specified in any case other than lower in create table is causing issues during insert
    sparkSession.sql(
      "CREATE TABLE testgreenraw.emp_data_hive (age INT, name STRING, ooid STRING) PARTITIONED BY (designation STRING, salary INT) STORED AS Parquet")
    empDf.write.insertInto("testgreenraw.emp_data_hive")

    sparkSession
      .sql("show create table testgreenraw.emp_data_plain_hive")
      .show(100, false)
    sparkSession.sql("show create table testgreenraw.emp_data_hive").show(100, false)
    sparkSession
      .sql("show create table testgreenraw.emp_data_plain_spark")
      .show(100, false)
    sparkSession.sql("show create table testgreenraw.emp_data_csv_spark").show(100, false)
    sparkSession.sql("show create table testgreenraw.emp_data_spark").show(100, false)
  }

  def testOptionsParser(argString: String) = {

    val args = argString.trim().split("\\s+")
    println(args.toList)
    val config                                = sparkDDLGeneratorOptions.parse(args)
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val configJSON: String                    = write(config)
    println(configJSON)
    config
  }

  def altertableLocation(implicit sparkSession: SparkSession) {
    val targetTable = "testgreenraw.emp_data_hive"

    val tableLocation = sparkSession
      .sql(s"describe formatted $targetTable")
      .filter(col("col_name") === "Location")
      .select("data_type")
      .take(1)(0)
      .getString(0)

    val newtableLocation = tableLocation + "_bkp"

    sparkSession.sql(s"""ALTER TABLE $targetTable set location "$newtableLocation"""")
    sparkSession.sql(s"show create table $targetTable").show(100, false)
    sparkSession.sql(s"describe formatted $targetTable").show(100, false)

    sparkSession
      .table("testgreenraw.emp_data_spark")
      .write
      .mode(SaveMode.Overwrite)
      .insertInto(targetTable)

  }

  def testMigrationDDlsByDb(dbname: String): List[SparkStyleTableMigration] = {
    val tablesList = sparkSession.catalog.listTables(dbname).collect().toList
    val migrationsList: List[SparkStyleTableMigration] =
      SparkDDLMigrator.generateMigrationDDLByTables(tablesList)
    migrationsList.foreach(migration => {
      println(
        s"*************${migration.databaseName}.${migration.tableName}***********************")
      println(s"HIVE_SCRIPT:\n${migration.hiveDDL}")
      println(s"SPARK_SCRIPT:\n${migration.sparkCreateTableSql}")
    })
    migrationsList
  }

  def testMigrationDDlsByTableNames(
      tableNames: List[String]): List[SparkStyleTableMigration] = {
    val tablesList = tableNames.map(sparkSession.catalog.getTable)
    val migrationsList: List[SparkStyleTableMigration] =
      SparkDDLMigrator.generateMigrationDDLByTables(tablesList)
    migrationsList.foreach(migration => {
      println(
        s"*************${migration.databaseName}.${migration.tableName}***********************")
      println(s"HIVE_SCRIPT:\n${migration.hiveDDL}")
      println(s"SPARK_SCRIPT:\n${migration.sparkCreateTableSql}")
    })
    migrationsList
  }

  def testS3Upload(
      migrationTables: List[SparkStyleTableMigration],
      sqlPrefix: String,
      s3BucketName: String) {
    SparkDDLMigrator.uploadBkpSqlsToS3(migrationTables, sqlPrefix, s3BucketName)
  }

  def testMigration(migrationTables: List[SparkStyleTableMigration]) {
    sparkSession
      .sql("show create table testgreenraw.emp_data_plain_hive")
      .show(100, false)
    sparkSession
      .sql(s"SHOW TBLPROPERTIES testgreenraw.emp_data_plain_hive")
      .show(100, false)
    sparkSession.sql("show create table testgreenraw.emp_data_hive").show(100, false)
    sparkSession.sql(s"SHOW TBLPROPERTIES testgreenraw.emp_data_hive").show(100, false)
    SparkDDLMigrator.migrateToSparkStyledTables(migrationTables)
    sparkSession
      .sql("show create table testgreenraw.emp_data_plain_hive")
      .show(100, false)
    sparkSession
      .sql(s"SHOW TBLPROPERTIES testgreenraw.emp_data_plain_hive")
      .show(100, false)
    sparkSession.sql("show create table testgreenraw.emp_data_hive").show(100, false)
    sparkSession.sql(s"SHOW TBLPROPERTIES testgreenraw.emp_data_hive").show(100, false)
  }

}
