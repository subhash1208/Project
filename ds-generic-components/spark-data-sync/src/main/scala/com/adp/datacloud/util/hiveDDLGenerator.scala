package com.adp.datacloud.util

import java.util.Date
import com.adp.datacloud.cli.hiveDDLGeneratorOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object hiveDDLGenerator {

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {
    
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val config = hiveDDLGeneratorOptions.parse(args)

    implicit val sparkSession: SparkSession =
      if (System
        .getProperty("os.name")
        .toLowerCase()
        .contains("windows") || System
        .getProperty("os.name")
        .toLowerCase()
        .startsWith("mac")) {
        // log events to view from local history server
        val eventLogDir = s"/tmp/spark-events/"
        scala.reflect.io
          .Path(eventLogDir)
          .createDirectory(force = true, failIfExists = false)

        // store hive tables in this location in local mode
        val warehouseLocation = s"/tmp/spark-warehouse/"
        scala.reflect.io
          .Path(warehouseLocation)
          .createDirectory(force = true, failIfExists = false)

        // use local mode for testing in windows
        SparkSession
          .builder()
          .appName(config.applicationName)
          .master("local[*]")
          .config("spark.eventLog.enabled", value = true)
          .config("spark.eventLog.dir", eventLogDir)
          .enableHiveSupport()
          .getOrCreate()

      } else {
        SparkSession
          .builder()
          .appName(config.applicationName)
          .enableHiveSupport()
          .getOrCreate()
      }

    val filesString = config.files
      .getOrElse("")
      .trim

    if (filesString.nonEmpty) {
      filesString
        .split(",")
        .foreach(x => {
          logger.info(s"DEPENDENCY_FILE: adding $x to sparkFiles")
          sparkSession.sparkContext.addFile(x)
        })
    }

    val ddlsArray =
      generateDDls(config.dbName, config.s3BucketPath, config.getGlueDbName())
    sparkSession.sparkContext
      .parallelize(ddlsArray)
      .saveAsTextFile(s"/tmp/${config.ddlFileName}")

  }

  def generateDDls(dbName: String, s3BucketPath: String, s3DbName: String)(
    implicit
    sparkSession: SparkSession): Array[String] = {

    val catalog = sparkSession.catalog
    catalog.setCurrentDatabase(dbName)
    val tablesList = catalog.listTables.collect()
    val validPropertiesList = List("parquet.compression")
    val currentDate = new Date
    val ddlMigrationProp = s"'DDL.migrated.on' = '$currentDate'"
    val regPatch = """(?s)OPTIONS.*?\)""".r

    tablesList.flatMap(table => {
      val tname = table.name
      val ttype = table.tableType
      if (ttype == "MANAGED") {
        val props = sparkSession
          .sql(s"SHOW TBLPROPERTIES $tname")
          .collect()
          .map(row => row.getAs[String]("key") -> row.getAs[String]("value"))
          .toMap
        // filter properties that are needed and ignore the rest like createTime,createdBy etc..
        val cleanedProps = ddlMigrationProp :: props
          .filter(x => validPropertiesList.contains(x._1))
          .map(x => s"'${x._1}'  = '${x._2}'")
          .toList
        // discard existing table properties and replace them with filtered properties
        val createTableStmt = sparkSession
          .sql(s"SHOW CREATE TABLE $dbName.$tname")
          .collect()
          .map(_.getAs[String]("createtab_stmt"))
          .mkString
          .replace(s"$dbName", s"$s3DbName")
          .replace("TABLE", "TABLE IF NOT EXISTS")
          .replace("USING parquet", "STORED AS parquet")
          .split("TBLPROPERTIES")(0)
        // this is to patch tables created by spark
        val createTableStmtPatched = if (createTableStmt.contains("OPTIONS")) {
          regPatch.replaceAllIn(createTableStmt, "")
        } else {
          createTableStmt
        }
        val s3Location = s"LOCATION '$s3BucketPath/$tname'"
        val tableProperties =
          cleanedProps.mkString("TBLPROPERTIES(\n", ",\n", "\n);")
        val repairTable = if (createTableStmtPatched.contains("PARTITIONED BY")) {
          s"MSCK REPAIR TABLE `$s3DbName`.`$tname`;"
        } else {
          s""
        }
        props.filter(x => validPropertiesList.contains(x._1))
        val fullStr =
          Seq(createTableStmtPatched, s3Location, tableProperties, repairTable)
            .mkString(s"-------$dbName.$tname-----\n", "\n", "\n")
        Some(fullStr)
      } else {
        None
      }
    })

  }
}