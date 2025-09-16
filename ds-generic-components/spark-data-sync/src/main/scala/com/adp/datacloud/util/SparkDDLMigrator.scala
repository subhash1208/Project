package com.adp.datacloud.util

import java.util.Date

import com.adp.datacloud.cli.sparkDDLGeneratorOptions
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.CannedAccessControlList
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.functions.col

import scala.sys.process._

case class SparkStyleTableMigration(
  databaseName:        String,
  tableName:           String,
  hiveDDL:             String,
  sparkCreateTableSql: String,
  isExternal:          Boolean,
  isPartitioned:       Boolean,
  isError:             Boolean = false)

object SparkDDLMigrator {
  private val logger = Logger.getLogger(getClass)

  val AlterToExternal = "alterToExternal_step1"
  val DropTable = "dropTable_step2"
  val CreateTable = "createTable_step3"
  val AlterToInternal = "alterToInternal_step4"

  def main(args: Array[String]) {
    
    println(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val config = sparkDDLGeneratorOptions.parse(args)

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

    val catalog = sparkSession.catalog
    val failures = if (config.dbNames.nonEmpty) {
      config.dbNames.flatMap(dbName => {
        val tablesList = catalog.listTables(dbName).collect().toList
        val tableMigrationsList = generateMigrationDDLByTables(tablesList, config.replaceMap)
        uploadBkpSqlsToS3(tableMigrationsList, config.sqlPrefix, config.s3bucketName)
        migrateToSparkStyledTables(tableMigrationsList)
      })
    } else {
      val tablesList = config.tableNames.map(catalog.getTable)
      val tableMigrationsList = generateMigrationDDLByTables(tablesList, config.replaceMap)
      uploadBkpSqlsToS3(tableMigrationsList, config.sqlPrefix, config.s3bucketName)
      migrateToSparkStyledTables(tableMigrationsList)
    }

    if (failures.nonEmpty) {
      throw new Exception(
        s"some of table failed to migrate FAILED_TABLES=${failures.mkString(",")}")
    }

  }

  def generateMigrationDDLByTables(
    tablesList: List[Table],
    replaceMap: Map[String, String] = Map())(implicit sparkSession: SparkSession): List[SparkStyleTableMigration] = {
    val catalog = sparkSession.catalog
    val validPropertiesList = List("parquet.compression", "DDL.migrated.on")
    val currentDate = new Date
    val ddlMigrationProp = s"'sparkDDL.migrated.on' = '$currentDate'"

    val metaDataList: List[SparkStyleTableMigration] = tablesList.par
      .flatMap(catalogTable => {
        val tableName = catalogTable.database + "." + catalogTable.name

        logger.info(s"PROCESSING table $tableName")
        try {
          // discard table properties
          val createTableStmt = sparkSession
            .sql(s"SHOW CREATE TABLE $tableName")
            .collect()
            .map(_.getAs[String]("createtab_stmt"))
            .mkString
            .toUpperCase
            .split("TBLPROPERTIES")(0)

          // only parquet hive styled tables are to be migrated
          if (createTableStmt.toUpperCase.contains(
            "ORG.APACHE.HADOOP.HIVE.QL.IO.PARQUET.SERDE.PARQUETHIVESERDE")) {
            val isExternalTable = catalogTable.tableType == CatalogTableType.EXTERNAL.name

            val props = sparkSession
              .sql(s"SHOW TBLPROPERTIES $tableName")
              .collect()
              .map(row => row.getAs[String]("key") -> row.getAs[String]("value"))
              .toMap

            // filter properties that are needed and ignore the rest like createTime,createdBy etc..
            val hiveTableProp = props
              .filter(x => validPropertiesList.contains(x._1))
              .map(x => s"'${x._1}'  = '${x._2}'")
              .toList

            val hiveTablePropertiesString = if (hiveTableProp.nonEmpty) {
              hiveTableProp.mkString("\nTBLPROPERTIES(\n", ",\n", "\n)")
            } else {
              s""
            }

            val hiveCreateTablestmt = if (isExternalTable) {
              sparkSession
                .sql(s"SHOW CREATE TABLE $tableName")
                .collect()
                .map(_.getAs[String]("createtab_stmt"))
                .mkString
                .toUpperCase
                .split("TBLPROPERTIES")(0)
                .trim() + hiveTablePropertiesString
            } else {
              logger.info(
                s"converting table: $tableName to external for DDL Extraction")
              convertTableType(tableName, CatalogTableType.EXTERNAL)
              val createTablesDDL = sparkSession
                .sql(s"SHOW CREATE TABLE $tableName")
                .collect()
                .map(_.getAs[String]("createtab_stmt"))
                .mkString
                .toUpperCase
                .split("TBLPROPERTIES")(0)
                .trim() + hiveTablePropertiesString
              logger.info(
                s"converting table: $tableName back to Managed after DDL Extraction")
              convertTableType(tableName, CatalogTableType.MANAGED)
              createTablesDDL
            }

            val sparkTableProps = ddlMigrationProp :: hiveTableProp
            val sparkTablePropertiesString =
              sparkTableProps.mkString("\nTBLPROPERTIES(\n", ",\n", "\n)")

            val tableLocation = sparkSession
              .sql(s"describe formatted $tableName")
              .filter(col("col_name") === "Location")
              .select("data_type")
              .take(1)(0)
              .getString(0)

            val sourceColumns = catalog.listColumns(tableName).collect().toList
            val partitionProjections =
              sourceColumns.filter(p => p.isPartition).map(_.name)

            //specifying the location creates an external table
            val sparkCreateTableStmt = sourceColumns
              .map(c => c.name + " " + c.dataType)
              .mkString(s"CREATE TABLE $tableName (", ",\n", ") USING parquet") + {
                if (partitionProjections.nonEmpty) {
                  partitionProjections.mkString("\nPARTITIONED BY (", ",\n", ")")
                } else ""
              } + s"\n LOCATION '$tableLocation'" + sparkTablePropertiesString

            val alterToExternalStmt =
              s"ALTER TABLE $tableName SET TBLPROPERTIES('EXTERNAL'='TRUE')"
            val dropTableStmt = s"DROP TABLE IF EXISTS $tableName"
            val alterToInternalStmt = if (isExternalTable) {
              s"-- $tableName is External"
            } else {
              s"ALTER TABLE $tableName SET TBLPROPERTIES('EXTERNAL'='FALSE')"
            }
            val recoverPatitionsStmt = if (partitionProjections.nonEmpty) {
              s"MSCK REPAIR TABLE $tableName"
            } else {
              s"-- $tableName has no partitions"
            }

            val hiveFallBackSql = List(
              s"---HIVE_BKP_SCRIPT: $tableName ---",
              alterToExternalStmt,
              dropTableStmt,
              hiveCreateTablestmt,
              alterToInternalStmt,
              recoverPatitionsStmt).mkString("", ";\n\n", ";")

            val replacedHiveFallBackSql = replaceMap.foldLeft(hiveFallBackSql)(
              (baseSql, pair) => baseSql.replaceAll(pair._1, pair._1))

            logger.info(s"DDL_EXTRACTION: extracted metadata for $tableName")
            println(s"DDL_EXTRACTION: extracted metadata for $tableName")

            Some(
              SparkStyleTableMigration(
                catalogTable.database,
                catalogTable.name,
                replacedHiveFallBackSql,
                sparkCreateTableStmt,
                isExternalTable,
                partitionProjections.nonEmpty))

          } else {
            None
          }

        } catch {
          case e: Exception => {
            logger.error(s"DDL_EXTRACTION: error while fetching metadata for table $tableName", e)
            Some(
              SparkStyleTableMigration(
                catalogTable.database,
                catalogTable.name,
                null,
                null,
                isExternal = false,
                isPartitioned = false,
                isError = true))
          }
        }

      })
      .toList

    val failedTables =
      metaDataList.filter(_.isError == true).map(_.tableName).mkString(",")
    println(s"failed fetching meta data for the following tables: $failedTables")
    logger.warn(
      s"failed fetching meta data for the following tables: $failedTables")

    val migrationsList = metaDataList.filter(_.isError == false)
    migrationsList
  }

  def uploadBkpSqlsToS3(
    migrationTables: List[SparkStyleTableMigration],
    sqlPrefix:       String,
    s3BucketName:    String)(implicit sparkSession: SparkSession): Unit = {

    val s3Client =
      AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build

    migrationTables.par.foreach(migrationTable => {
      val s3FileName =
        s"hiveTableBackUpScripts/$sqlPrefix/${migrationTable.databaseName}/${migrationTable.tableName}.sql"
      val s3FilePath = s"s3://$s3BucketName/$s3FileName"
      try {
        s3Client.putObject(s3BucketName, s3FileName, migrationTable.hiveDDL)
        s3Client.setObjectAcl(
          s3BucketName,
          s3FileName,
          CannedAccessControlList.BucketOwnerFullControl)
        logger.info(s"DDL_UPLOAD: uploaded ddl to $s3FilePath")
        println(s"DDL_UPLOAD: uploaded ddl to $s3FilePath")
      } catch {
        case e: Exception => logger.error(s"DDL_UPLOAD: error while uploading $s3FilePath", e)
      }
    })
  }

  def migrateToSparkStyledTables(
    migrationTables: List[SparkStyleTableMigration])(implicit sparkSession: SparkSession): List[String] = {

    val failedTables = migrationTables.par
      .flatMap(migrationTable => {
        val tableFullName = migrationTable.databaseName + "." + migrationTable.tableName
        try {
          convertTableType(tableFullName, CatalogTableType.EXTERNAL)
          // to avoid data loss make sure the table is external
          assert(
            sparkSession.catalog
              .getTable(tableFullName)
              .tableType == CatalogTableType.EXTERNAL.name)
          sparkSession.sql(s"DROP TABLE $tableFullName")
          sparkSession.sql(migrationTable.sparkCreateTableSql)
          if (!migrationTable.isExternal) {
            convertTableType(tableFullName, CatalogTableType.MANAGED)
          }
          if (migrationTable.isPartitioned) {
            sparkSession.sql(s"MSCK REPAIR TABLE $tableFullName")
          }
          logger.info(s"SPARK_MIGRATE: sucessfully migrated table $tableFullName")
          println(s"SPARK_MIGRATE: sucessfully migrated table $tableFullName")

          None
        } catch {
          case e: Exception => {
            logger.error(s"SPARK_MIGRATE: error while migrating table $tableFullName", e)
            Some(tableFullName)
          }
        }
      })
      .toList
    failedTables
  }

  def convertTableType(tableName: String, catlalogTableType: CatalogTableType)(
    implicit
    sparkSession: SparkSession): Unit = {
    val tableIdentifier =
      sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    val oldTable =
      sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    val alteredTable = oldTable.copy(tableType = catlalogTableType)
    sparkSession.sessionState.catalog.alterTable(alteredTable)
  }

  @deprecated("avoid using commandline", "09-01-2020")
  def executeMigrationSqls(sqlPrefix: String)(
    implicit
    sparkSession: SparkSession): Unit = {

    // Order is important, using List retains the order.
    List(AlterToExternal, DropTable, CreateTable, AlterToInternal).foreach(
      name => {
        val sqlName = sqlPrefix + s"_$name.sql"
        name match {
          case AlterToExternal | AlterToInternal => {
            val sqlCmd = s"hive -v -f /tmp/$sqlName"
            logger.info(s"CLI_CMD: $sqlCmd")
            println(s"CLI_CMD: $sqlCmd")
            println(sqlCmd.!!)
          }
          case DropTable => {
            val sourceFile = scala.io.Source.fromFile(s"/tmp/$sqlName")
            val sqlString = try sourceFile.mkString
            finally sourceFile.close()
            sqlString
              .split(";")
              .foreach(dropSql => {
                val tableNamepattern = """(\w+\.\w+)""".r
                val tableName = tableNamepattern.findFirstIn(dropSql).get
                // make sure that the table is external before dropping as dropping an internal table might result in data loss
                assert(
                  sparkSession.catalog
                    .getTable(tableName)
                    .tableType == CatalogTableType.EXTERNAL.name)
                logger.info(s"Running DROP_SQL: $dropSql")
                println(s"Running DROP_SQL: $dropSql")
                sparkSession.sql(dropSql)
              })
          }
          case CreateTable => {
            val sourceFile = scala.io.Source.fromFile(s"/tmp/$sqlName")
            val sqlString = try sourceFile.mkString
            finally sourceFile.close()
            sqlString
              .split(";")
              .foreach(createSql => {
                logger.info(s"Running CREATE_SQL: $createSql")
                println(s"Running CREATE_SQL: $createSql")
                sparkSession.sql(createSql)
              })
          }
        }
      })
  }
}