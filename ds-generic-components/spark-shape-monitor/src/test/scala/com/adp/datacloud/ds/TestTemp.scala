package com.adp.datacloud.ds

import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.adp.datacloud.cli.{ShapeMonitorConfig, ShapeMonitorOptions}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

class TestTemp {

  private val logger = Logger.getLogger(getClass())

  def main(args1: Array[String]) {

    //val args:Array[String] = "--xml-config-file src/test/resources/ev4_shape_monitor_config.xml --db-jdbc-url cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1 --db-username ADPI_DXR --db-password adpi --oracle-wallet-location /app/oraclewallet".split(" ")
    val args: Array[String] =
      "--xml-config-file src/test/resources/ev4_shape_monitor_config.xml --db-jdbc-url jdbc:oracle:thin:@cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1 --db-username ADPI_DXR --db-password adpi"
        .split(" ")

    val ShapeMonitorConfig = ShapeMonitorOptions.parse(args)

    System.setProperty("hadoop.home.dir", "/Users/somapava/Documents/ADP_Work/hive")
    implicit val sparkSession = SparkSession
      .builder()
      .appName(ShapeMonitorConfig.applicationName)
      .config("spark.sql.warehouse.dir", "/Users/somapava/Documents/ADP_Work/hive")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    println(sparkSession.sparkContext.getConf.toDebugString)

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    println(sdf.format(new Date))

    println(ShapeMonitorConfig.xmlFilePath)

    println(s"${ShapeMonitorConfig.dbJdbcUrl.getOrElse("")}")

    val query1 = s"""(SELECT 
                       region_code,
                       count(1) as count
                     FROM
                       optout_master
                     GROUP BY
                       region_code)
                  """

    val optout = sparkSession.read
      .format("jdbc")
      .option("url", ShapeMonitorConfig.dbJdbcUrl.getOrElse(""))
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      //  .option(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION, ShapeMonitorConfig.oracleWalletLocation)
      .option("user", "ADPI_DXR")
      .option("password", "adpi")
      .option("dbtable", query1)
      .load()

    // optout.show()

    // optout.schema.fields.foreach(println)
    // optout.schema.fields

    //  saveStatistics(optout, ShapeMonitorConfig)

    val tbl = "Employee_monthly".toLowerCase()

    val statsTableDf: DataFrame = sparkSession.read
      .format("jdbc")
      .option("url", ShapeMonitorConfig.dbJdbcUrl.getOrElse(""))
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      //  .option(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION, ShapeMonitorConfig.oracleWalletLocation)
      .option("user", "ADPI_DXR")
      .option("password", "adpi")
      .option(
        "dbtable",
        s"""(SELECT
                                 CONCAT(CONCAT(partition_spec, ' = '), partition_value) as partition, 
                                 CASE WHEN LTRIM(RTRIM(subpartition_spec)) is not null 
                                     THEN CONCAT(CONCAT(subpartition_spec,' = '), subpartition_value) 
                                 END as subpartition 
                               FROM
                                 (SELECT DISTINCT
                                       partition_spec,
                                       partition_value,
                                       subpartition_spec,
                                       subpartition_value 
                                  FROM TABLE_STATISTICS 
                                  WHERE LOWER(table_name) = '${tbl}') ) alias""")
      .load()

    //statsTableDf.show(false)

    val transaction = sparkSession.range(10).select($"id")

    val schema = transaction.schema.fields(0).dataType

    //Thread.sleep(100)

    val tables_list = List("ev5_dim_client", "ev4_dim_client", "totalsource_dim_client")

    val str = tables_list
      .map(x => {
        val yyyymm    = "201909"
        val commoncol = "clnt_obj_id"
        val finalSql  = s"""
              SELECT
                table_name,
                blue_raw_count,
                blue_raw_uniq_ooid_cnt,
                blue_optout_uniq_ooid_cnt,
                blue_raw_actual_optout_uniq_ooid_cnt,
                raw_uniq_ooid_cnt
              FROM	
              (select 
                '$x' as table_name,
                count(1) as blue_raw_count,
                count(distinct blue.clnt_obj_id) as blue_raw_uniq_ooid_cnt,
                count(distinct optout.clnt_obj_id) as blue_optout_uniq_ooid_cnt,
                count(distinct case when optout.clnt_obj_id is null then blue.clnt_obj_id else null end) as blue_raw_actual_optout_uniq_ooid_cnt
              from
              (select 
                  $commoncol as clnt_obj_id
              from
              dc1adsbluelanding.$x 
              where ingest_month='$yyyymm'
              )blue 
              left join
              (select distinct a.$commoncol as clnt_obj_id
              from
              dc1dsraw.optout_base_201909 b
              inner join
              dc1dsraw.optout_master a
              on a.region_code = b.service_center and
                 a.company_code = b.company_code and
                 a.product_code = b.product_code and 
                 a.clnt_obj_id <> '') optout
              on blue.clnt_obj_id = optout.clnt_obj_id) landing
              inner join
              (select 
                  count(distinct $commoncol) as raw_uniq_ooid_cnt
              from
              dc1dsraw.$x 
              where ingest_month='$yyyymm')raw
          """
        finalSql
      })
      .mkString("\n UNION ALL \n")

    //println(str)

    /*val xmlProcessor = XmlInputProcessor(ShapeMonitorConfig.xmlFilePath)

       xmlProcessor.tablenames.foreach(println)

       val shapeConfigurations =  xmlProcessor.getShapeDetailsFromXml()

       println(shapeConfigurations)

       val shapeConfigStr = shapeConfigurations.toDF().toJSON

       println(shapeConfigStr.collect().mkString("\n"))*/

    /*
       println("\n Printing virtual columns details \n")
       val hiveTableColumnsList = List("clnt_obj_id","kumar")
       tablesConfig.foreach(x =>{
         println(s"table name :" + x.tablename )
         val virtualColumnsList = x.virtualcolumns match {
           case Some(vcolumn) => vcolumn.map( y => y._2 + " as " + y._1)
           case None          => List()
         }

         val finalColumnsList = (x.excludecolumns, x.strictcolumns) match {
          case (Some(ecolumn), None)          => hiveTableColumnsList.diff(ecolumn) ++ virtualColumnsList
          case (_, Some(scolumn))             => hiveTableColumnsList.intersect(scolumn) ++ virtualColumnsList
          case (None, None)                   => hiveTableColumnsList ++ virtualColumnsList
        }

         println(finalColumnsList.mkString(","))

         val finalSql = s"select ${finalColumnsList.mkString(",")} from ${x.dbname}.${x.tablename}"
         println(finalSql)
       })*/

    val checkl = List()
    //if ( checkl.isEmpty ) println("It is empty List") else println("It is non-empty list")

    val list1 = List("soma", "pavan", "kumar")
    val list2 = List("pava", "suman")

    //println(list2.intersect(list1))

    /*val existingData = getComputedPartitionsDtls(List("yyyymm"), ShapeMonitorConfig, "employee_monthly")
      existingData.show(false)
      val condtion = existingData.collect().map(x => List(x(0).toString(), x(1).toString()).mkString(" not in ")).mkString.replaceAll("^$", "1 = 1")
      val wcondtion = condtion match {
        case "" =>  " "
        case _  =>  s"where $condtion"
       }

      println("Final Sql Condition is : " + wcondtion)
      val finalDf1 = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")
      val finalDf = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset").where(s"${condtion}").count()
      println(finalDf1.count())
      println(finalDf)*/

    //val df = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")

    //df.createOrReplaceTempView("employee_monthly")

    /*val finalSql = s"""select employee_guid, source_hr, source_pr, ooid, ap_region_code, hire_date_,
        regular_pay_, hourly_rate_, overtime_hours_, case when annual_base_band_ <> 'Unknown' then  annual_base_band_ end annual_base_band_clean,
        crc32(ssn) as ssn_, yyyymm from employee_monthly"""
     */

    //val finalSql = s"""select employee_guid from employee_monthly"""

    //val dataDf = sparkSession.sql(finalSql)
    //dataDf.show()

    /*val shapeConfiguration = ShapeConfiguration("distdsbase","employee_monthly",List(),List(),(List(("annual_base_band_", "case when annual_base_band_ <> 'Unknown' then  annual_base_band_ end"),("ssn","crc32(ssn)"))) )
      println(shapeConfiguration)
      val finalDf = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")
        .select("employee_guid","source_pr","source_hr","ooid","ssn","ap_region_code","annual_base_band_","regular_temporary_","hire_date_","annual_base_","gross_wages_","yyyymm")
        .filter(col("source_pr").isNotNull)

      val partitionCol = Option(List("yyyymm"))
      val statCollector = new DeequStatsCollector(finalDf, shapeConfiguration, partitionCol)
      statCollector.collect().show()*/

    //columnProfilesList.map(_._2.profiles).foreach(println)

    /*      val finalDf = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")
        .select("employee_guid","source_pr","source_hr","ooid","ssn","ap_region_code","annual_base_band_","regular_temporary_","hire_date_","annual_base_","gross_wages_","yyyymm")
        .filter(col("source_pr").isNotNull)

      val pcolumns =  List("yyyymm","source_pr")

      val pcombo=
          finalDf.rollup(pcolumns.map(c=> col(c)) : _*)
         .count()
         .filter(s"${pcolumns.head} is not null")
         .drop("count")
         .collect
         .map(_.toSeq
               .map(x => x match {
                                  case x if  x.isInstanceOf[Any] => x.toString()
                                  case x if !x.isInstanceOf[Any] => "-1"}))
         .map(x => for { i <- 0 until x.size
                         j <- 0 until pcolumns.length} yield { if (i == j & !x(i).equals("-1"))  s"${pcolumns(j)} = '${x(i)}'" else s"" }
              ).map(_.toList.filter(!_.isEmpty())).toList
       //pcombo.show(false)  //.map(x => x.toList.distinct.diff(List("NA"))).toList
       println(pcombo)

       val fExpr = pcombo.map(_.reduce(_ + " and " + _))

       println(fExpr)

       val counts = fExpr.map(finalDf.filter(_).count())
       println(counts)
     */

    //val partitionColumns = List("yyyymm","source_hr","source_pr")
    /*val partitionColumns: List[String] = List()
      println(generateGroupingSets(Option(List("yyyymm","source_hr","source_pr"))))
      println(partitionColumns.toSet.subsets().map(_.toList).filter(_.contains(partitionColumns.head)).toList)
      println(partitionColumns.toSet.subsets().map(_.toList).filter(_.contains(partitionColumns.head)).toList.mkString(",").replaceAll("List", ""))*/

    /*val list_1 = List("a","b","c","d")
     println(list_1.take(1))
     println("take first element from empty list " + List().take(1) )*/

    /*val pcolumns = List("yyyymm")
      val computedPartitions = getComputedPartitionsDtls(pcolumns, ShapeMonitorConfig, "ditdsmain", "employee_monthly_201908")
      computedPartitions.show()
      val computedPartitionsFilter = computedPartitions.collect().map(x => List(x(0).toString(), x(1).toString()).mkString(" not in ")).mkString.replaceAll("^$", "1 = 1")
      println("Filter conidtion is "+ computedPartitionsFilter)*/

    /*val finalDf1 = sparkSession.read.parquet("src/test/resources/employee_monthly_shapetest_dataset")
      finalDf1.show(false)

      import com.amazon.deequ.{VerificationSuite, VerificationResult}
      import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
      import com.amazon.deequ.checks.{Check, CheckLevel}


      val verificationResult: VerificationResult = { VerificationSuite()
        .onData(finalDf1)
        .addCheck(
            Check(CheckLevel.Error, "Employee Data Test")
            .isPrimaryKey("employee_guid")
            .isUnique("ssn")
            .isContainedIn("source_hr", Array("EV5","VANTAGE","WFN")))
        .run()
      }

      val resultDataFrame = checkResultsAsDataFrame(sparkSession, verificationResult)

      resultDataFrame.show(false)*/

    println("\n\n")
    val mapData = Map(
      "10.48.236.88/liveupdate.symanticliveupdate.com" -> "1576820057709",
      "10.48.236.180/liveupdate.symanticliveupdate.com" -> "1576820387923")
    println(mapData)
    val listData = mapData.map(x => List(x._1, x._2)).toList
    println(listData)
    println("\n")
    val textData = listData.map(_.mkString(",")).mkString("\r\n")
    println(textData)
    println("\n\n")
  }

  def getComputedPartitionsDtls(
      partitionColumns: List[String],
      ShapeMonitorConfig: ShapeMonitorConfig,
      dbname: String,
      tablename: String)(implicit sparkSession: SparkSession) = {
    /*
    val tableDf = spark.read
      .format("jdbc")
      .option("url", s"jdbc:oracle:thin:ADPI_DXR/adpi@cdldfoaff-scan.es.ad.adp.com:1521/cri03q_svc1")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("user", "ADPI_DXR")
      .option("password", "adpi")
      .option("dbtable", "COLUMN_STATISTICS")
      .load()
    tableDf.write.mode("overwrite").saveAsTable("ditdsraw.column_statistics")
     */

    println(ShapeMonitorConfig.jdbcProperties.asScala.toMap)
    // Read if any existing stats are already calculated for the given table
    val statsTableDf: DataFrame = sparkSession.read
      .format("jdbc")
      .options(ShapeMonitorConfig.jdbcProperties.asScala.toMap)
      .option(
        "dbtable",
        s"""(SELECT
                               partition_spec,
                               '(''' || LISTAGG(partition_value,''',''') WITHIN GROUP (ORDER BY partition_value) || ''')' as partition_value
                             FROM
                               (SELECT DISTINCT
                                     partition_spec,
                                     partition_value 
                                FROM ${ShapeMonitorConfig.targetTable} 
                                WHERE LOWER(database_name)              = '${dbname}'         AND
                                      LOWER(table_name)                 = '${tablename}'      AND
                                      LOWER(partition_spec)             = '${partitionColumns.head}' AND
                                      NVL(LOWER(subpartition_spec),' ') = NVL('${partitionColumns.tail.mkString}',' '))
                             GROUP BY partition_spec ) alias""")
      .load()

    statsTableDf
  }

  def saveStatistics(
      statisticsDataFrame: DataFrame,
      ShapeMonitorConfig: ShapeMonitorConfig) {
    try {
      statisticsDataFrame.write
        .format("jdbc")
        .option("url", s"jdbc:oracle:thin:@${ShapeMonitorConfig.dbJdbcUrl}")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .mode(SaveMode.Append)
        //   .option(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION, ShapeMonitorConfig.oracleWalletLocation)
        .option("user", "ADPI_DXR")
        .option("password", "adpi")
        .option("dbtable", "TABLE_STATISTICS")
        .save()

      logger.info("Statistics are successfully written to TABLE_STATISTICS table")
    } catch {
      case e: Exception =>
        logger.error(s"Failed while writing statistics to rdbms table: ", e)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
    }
  }

  // This generate grouping sets with all the partitions present in the table
  def generateGroupingSets(
      partitioncolumns: Option[List[String]]): Option[List[List[String]]] = {

    partitioncolumns match {
      case Some(partitions) =>
        partitions.length match {
          case 1 => Some(List(partitions))
          case _ =>
            Some(
              partitions.tail.toSet.subsets.toList
                .map(x => (List(partitions.head) ++ x).toList)
                .map(_.map(_.toString())))
        }

      case None => None
    }
  }

}
