package com.adp.datacloud.ds.rdbms

import com.adp.datacloud.cli.{IncrementalExporterConfig, dxrParallelDataExporterOptions}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io._
import scala.io.Source

object incrementalDataExporter {

  private val logger = Logger.getLogger(getClass())
  
  def executeSparkSql(sqlString: String) (implicit sparkSession: SparkSession) = {
    println(sqlString)
    sparkSession.sql(sqlString)
  }

  def main(args: Array[String]) {

    logger.info(s"DS_GENERICS_VERSION: ${getClass.getPackage.getImplementationVersion}")

    val deltaCaptureConfig = com.adp.datacloud.cli.incrementalDataExporterOptions.parse(args)

    implicit val sparkSession: SparkSession =
      SparkSession
        .builder()
        .appName(deltaCaptureConfig.applicationName)
        .enableHiveSupport()
        .getOrCreate()

    val filesString = deltaCaptureConfig.files
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
    runIncrementalExport(deltaCaptureConfig)
  }

  protected def runIncrementalExport(deltaCaptureConfig: IncrementalExporterConfig)(implicit sparkSession: SparkSession) = {
    logger.info(s"INCREMENTAL_EXPORT_XML: ${deltaCaptureConfig.xmlContent}")
    // Process the XML configuration file and validate arguments
    val xmlProcessor = XmlInputProcessor(deltaCaptureConfig.xmlContent)

    for (deltaConfigurations <- xmlProcessor.getDeltaDetailsFromXml()) {

      val dbname = deltaConfigurations.hiveDbName
      val tgtSchemaName = deltaConfigurations.tgtSchemaName
      val rdbmsTableName = deltaConfigurations.rdbmsTableName
      val hiveSourceTableName = deltaConfigurations.hiveSourceTableName
      val hiveTgtTableName = deltaConfigurations.hiveTgtTableName
      val pkColumnsList = deltaConfigurations.pkColumnNames

      val excludedColumns = deltaConfigurations.excludedColumnName ++ List("db_schema")

      // In case of initial load , postgre dataset table may not exist , so below logic to have this
      //ex:- t_dim_orgn will be created like post gre from t_dim_orgn_full
      executeSparkSql("CREATE TABLE IF NOT EXISTS " + dbname + "." + hiveTgtTableName + " USING PARQUET AS SELECT * FROM " + dbname + "." + hiveSourceTableName + " where 1=0")

      // below is to get the excluded column from , but this will occurs on
      val sourceColumns = executeSparkSql("select * from  " + dbname + "." + hiveSourceTableName).drop(excludedColumns: _*).schema.fieldNames.toList
      val targetColumns = executeSparkSql("select * from  " + dbname + "." + hiveTgtTableName).drop(excludedColumns: _*).schema.fieldNames.toList

      // verify if a new column gets added in T_dim_orgn_full , in that case t_dim_orgn will be dropped and created so that new column will propagate as well as
      // full load will be triggered.
      if (sourceColumns.toSet.size == targetColumns.toSet.size && sourceColumns.toSet.diff(targetColumns.toSet).isEmpty && deltaCaptureConfig.incrementalFlag) {
        logger.info(s"Incremental processing possible")

        /* TEMPORARILY DISABLING SYNC status check to avoid the below error
         * ERROR: array size exceeds the maximum allowed (1073741823)
         *
        // Check further if source and target tables are in sync. We do this by performing a sync hash comparison
        val validationSql = generateValidationScript(tgtSchemaName, rdbmsTableName, pkColumnsList)
      	writeFile("presync_validation.sql", validationSql)

        val syncStatusExtractOptions = s"""--product-name ${deltaCaptureConfig.dxrProductName}
      	    --dxr-jdbc-url ${deltaCaptureConfig.dxrJdbcUrl}
      		  --source-db ${deltaCaptureConfig.targetdb}
      		  --landing-db ${deltaCaptureConfig.landingDatabaseName}
      		  --blue-db ${deltaCaptureConfig.blueDatabaseName}
      		  --green-db ${deltaCaptureConfig.greenDatabaseName}
      		  --sql-files presync_validation
      		  --optout-master ${deltaCaptureConfig.landingDatabaseName}.optout_master
      		  --oracle-wallet-location ${deltaCaptureConfig.oracleWalletLocation match { case Some(x) => x case None => "none" }}
      		  """.trim().stripMargin.split("\\s+")

        logger.info("Validation SQL Extraction Command")
      	logger.info(syncStatusExtractOptions.mkString(" "))
      	dxrParallelDataExtractor.runExtract(dxrParallelDataExtractorOptions.parse(syncStatusExtractOptions))
      	*/

      } else {
        executeSparkSql("DROP TABLE IF EXISTS " + dbname + "." + hiveTgtTableName)
        executeSparkSql("CREATE TABLE IF NOT EXISTS " + dbname + "." + hiveTgtTableName + " USING PARQUET AS SELECT * FROM " + dbname + "." + hiveSourceTableName + " where 1=0")
      }

      // drop the temporary tables if they exist
      executeSparkSql("DROP TABLE IF EXISTS  " + dbname + "." + hiveTgtTableName + "_upserts")
      executeSparkSql("DROP TABLE IF EXISTS  " + dbname + "." + hiveTgtTableName + "_deletes")
      executeSparkSql("DROP TABLE IF EXISTS  " + dbname + "." + hiveTgtTableName + "_old")

      //the below staging table will contain the delta data , this will be for insert/upsert
      executeSparkSql("CREATE TABLE " + dbname + "." + hiveTgtTableName + "_old USING PARQUET AS SELECT * FROM " + dbname + "." + hiveTgtTableName)

      val joinCondition = sourceColumns.map(x => List("new." + x, "old." + x)).map(_.reduce((a, b) => a + " <=> " + b)).reduce((a, b) => a + " and " + b)
      executeSparkSql("CREATE TABLE IF NOT EXISTS " + dbname + "." + hiveTgtTableName + "_upserts" + " USING PARQUET as  select new.*  from " + dbname + "." + hiveSourceTableName + " as new left anti join " + dbname + "." + hiveTgtTableName + "_old as old on " + joinCondition)

      //the below staging table will contain the delta data , this will be for delete and this should be based on pks
      val pkColumns = pkColumnsList.map(x => "old." + x).mkString(",")
      val pkJoinCondition = pkColumnsList.map(x => List("new." + x, "old." + x)).map(_.reduce((a, b) => a + "<=>" + b)).reduce((a, b) => a + " and " + b)

      executeSparkSql("CREATE TABLE IF NOT EXISTS " + dbname + "." + hiveTgtTableName + "_deletes " + " USING PARQUET as  select distinct " + pkColumns + " from " + dbname + "." + hiveTgtTableName + "_old as old left anti join " + dbname + "." + hiveSourceTableName + " as new  on " + pkJoinCondition)

      // data export call args should be built
      val preSqlFileName = "pre_sql_" + rdbmsTableName + ".sql"

      // TODO: The below pre-sql doesn't work for oracle. This code needs to be refactored.
      val preSql =
        s"""DROP TABLE IF EXISTS ${tgtSchemaName}.${rdbmsTableName}_UPSERTS;
        DROP TABLE IF EXISTS ${tgtSchemaName}.${rdbmsTableName}_DELETES;
        CREATE TABLE ${tgtSchemaName}.${rdbmsTableName}_UPSERTS AS SELECT * FROM ${tgtSchemaName}.${rdbmsTableName} WHERE 1=0;
        CREATE TABLE ${tgtSchemaName}.${rdbmsTableName}_DELETES AS SELECT ${pkColumns} FROM ${tgtSchemaName}.${rdbmsTableName} as old WHERE 1=0;
      """
      writeFile(preSqlFileName, preSql)

      val postSqlFileName = "post_sql_" + rdbmsTableName + ".sql"
      val postSql = generateMergeScript(tgtSchemaName, rdbmsTableName, pkColumnsList, sourceColumns, deltaCaptureConfig.incrementalFlag, deltaCaptureConfig.jdbcDBType)
      writeFile(postSqlFileName, postSql)

      val hiveTableUpserts = dbname + "." + hiveTgtTableName + "_upserts"
      val targetTableUpserts = tgtSchemaName + "." + rdbmsTableName + "_upserts"
      val argsUpserts =
        s"""--product-name ${deltaCaptureConfig.dxrProductName}
	    --dxr-jdbc-url ${deltaCaptureConfig.dxrJdbcUrl}
		--target-db ${deltaCaptureConfig.jdbcDBType}
		--oracle-wallet-location ${deltaCaptureConfig.oracleWalletLocation}
		--pre-export-sql ${preSqlFileName}
		--hive-table $hiveTableUpserts
		--target-rdbms-table $targetTableUpserts
		--use-staging-table False""".trim().stripMargin.split("\\s+")

      dxrParallelDataExporter.runExport(dxrParallelDataExporterOptions.parse(argsUpserts))

      val hiveTableDeletes = dbname + "." + hiveTgtTableName + "_deletes"
      val targetTableDeletes = tgtSchemaName + "." + rdbmsTableName + "_deletes"

      val argsDeletes =
        s"""--product-name ${deltaCaptureConfig.dxrProductName}
	  --dxr-jdbc-url ${deltaCaptureConfig.dxrJdbcUrl}
		--target-db ${deltaCaptureConfig.jdbcDBType}
		--oracle-wallet-location ${deltaCaptureConfig.oracleWalletLocation}
		--post-export-sql ${postSqlFileName}
		--hive-table $hiveTableDeletes
		--target-rdbms-table $targetTableDeletes
		--use-staging-table False""".trim().stripMargin.split("\\s+")

      dxrParallelDataExporter.runExport(dxrParallelDataExporterOptions.parse(argsDeletes))

      // data needs to be overwritten into targettable in hive
      executeSparkSql("INSERT OVERWRITE TABLE " + dbname + "." + hiveTgtTableName + "   select * from " + dbname + "." + hiveSourceTableName)

      // The "_upserts, _deletes and _old" tables need to be dropped after successful export.
      // However, we will not do so, in order to aid debugging in case something goes wrong.

    }
  }

  def generateValidationScript(tgtSchemaName: String,
                               tableName: String,
                               pkColumnsList: List[String],
                               targetDb: String = "postgres") = {
    val fileName = s"${targetDb}_presync_validation.sql" 
    val template = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString
    val replacements = Map("tableName" -> s"${tgtSchemaName}.${tableName}".toUpperCase(),
        "pkColumnsList" -> pkColumnsList.mkString(",").toUpperCase())
    
    replacements.foldLeft(template)((accumulator, replacementPair) => {
      ("\\{\\{" + replacementPair._1 + "\\}\\}").r.replaceAllIn(accumulator, replacementPair._2)
    })
  }
  
  // Generates the appropriate merge script
  def generateMergeScript(tgtSchemaName: String, 
      tableName: String,
      pkColumnsList: List[String],
      columnsList: List[String],
      incrementalFlag: Boolean, 
      targetDb: String = "postgres") = {
    
    val fileType = if (incrementalFlag) "merge" else "truncate_reload"
    val fileName = s"${targetDb}_${fileType}.sql" 
    val template = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName)).mkString
    val replacements = Map("tableName" -> s"${tgtSchemaName}.${tableName}".toUpperCase(),
        "pkColumnsList" -> pkColumnsList.mkString(",").toUpperCase(),
        "columnAssignments" -> (targetDb match {
          case "oracle" => "???" // TODO: Complete this
          case "postgres" => columnsList.filter(_ != "db_schema").map(x => x + " = excluded." + x).mkString(",\n\t")
        }).toUpperCase())
    
    replacements.foldLeft(template)((accumulator, replacementPair) => {
      ("\\{\\{" + replacementPair._1 + "\\}\\}").r.replaceAllIn(accumulator, replacementPair._2)
    })
    
  }

  def dxrConfigParse(
    tableName: String,
    config: IncrementalExporterConfig, exportType: String) = {
    // place your oracle wallet in home directory for testing
    val hiveTable = tableName
    val targetTable = tableName
    val args =
      s"""--product-name ${config.dxrProductName}
		""".trim().stripMargin.split("\\s+")
    args
  }
  
  
  def writeFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }

}