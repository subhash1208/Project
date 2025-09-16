package com.adp.datacloud.wrapper

import com.adp.datacloud.cli.sqlWrapperOptions
import com.adp.datacloud.util.SparkTestWrapper
import com.adp.datacloud.writers.HiveDataFrameWriter
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

case class EMP(Name: String, Age: Int, Designation: String, Salary: Int, Ooid: String)

@RunWith(classOf[JUnitRunner])
class SqlWrapperTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  override def beforeAll(): Unit = {
    super.beforeAll()
    cleanAndSetupDatabase("cdldsraw1")
    cleanAndSetupDatabase("cdldsraw2")
    createTestData
    displayAllTables("cdldsraw1")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("cdldsraw2")
  }

  val config = testParseConfig(true)

  private def createTestData() {
    import sparkSession.implicits._

    val emp1 = EMP("Solid Snake", 21, "MT", 2000, "56798")
    val emp2 = EMP("Big Boss", 62, "MT", 2000, "56797")
    val emp3 = EMP("Revolver Ocelot", 23, "MT", 2000, "56796")
    val emp4 = EMP("Liquid Snake", 24, "SMT", 3000, "56795")
    val emp5 = EMP("EVA", 25, "SMT", 3000, "56794")
    val emp6 = EMP("Raiden", 26, "SMT", 4000, "56793")
    val emp7 = EMP("Venom Snake", 27, "SMT", 4000, "56792")
    val emp8 = EMP("Skull Face", 37, "SMT", 4000, null)
    val emp9 = EMP("Quite", 24, "SMT", 4000, "56792")

    val empDf = List(emp1, emp2, emp3, emp4, emp5, emp6, emp7, emp8, emp9).toDF()
    empDf.show()

    empDf.write.saveAsTable("cdldsraw1.emp_data_plain")
    empDf.write
      .format("csv")
      .option("delimiter", "\t")
      .saveAsTable("cdldsraw1.emp_data_text")
    sparkSession.sql("show create table cdldsraw1.emp_data_text").show(500, false)
    empDf.write.partitionBy("Designation", "Salary").saveAsTable("cdldsraw1.emp_data")

    val hiveWriter = HiveDataFrameWriter("parquet", Some("Designation,Salary"))
    val fields     = hiveWriter.getColumnDefinitions(empDf.schema)
    val hiveDDL =
      hiveWriter.getCreateHiveTableDDL("cdldsraw1.emp_data_hive", fields, None)
    sparkSession.sql(hiveDDL)
    hiveWriter.insertOverwrite("cdldsraw1.emp_data_hive", empDf)

    val hiveWritertext = HiveDataFrameWriter("text", None)
    val fields2        = hiveWritertext.getColumnDefinitions(empDf.schema)
    val hiveDDL2 =
      hiveWritertext.getCreateHiveTableDDL("cdldsraw1.emp_data_text_hive", fields, None)
    sparkSession.sql(hiveDDL2)
    hiveWritertext.insertOverwrite("cdldsraw1.emp_data_text_hive", empDf)

  }

  def testParseConfig(
      optimizeInsert: Boolean = true,
      sqlName: String         = s"sql_wrapper_test") = {

    val inputSqlName = s"$sqlName.sql"
    val inputSqlPath =
      new java.io.File(s"src/test/resources/$inputSqlName").getCanonicalPath

    // add files to sparkContext
    sparkSession.sparkContext.addFile(inputSqlPath)

    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    val sqlWrapperArgs =
      s"""--file $inputSqlName
    --optimize-insert $optimizeInsert
    --hiveconf 1=2
    """.stripMargin.split("\\s+")

    val config = sqlWrapperOptions.parse(sqlWrapperArgs)
    println(config.sql)
    config
  }

  test("testSqlWrapper") {
    sqlWrapper.executeInputSql(config)
  }

  test("selectExtractTest") {
    val select1 =
      """insert overwrite table cdldsraw2.emp_data partition(designation,salary) SELECT * FROM (SELECT * FROM cdldsraw1.emp_data distribute by Ooid,Age)"""
    val select2 =
      """insert overwrite table cdldsraw2.emp_data partition(designation,salary)
			  |SELECT *
			  |FROM 
			  |    (SELECT *
			  |    FROM cdldsraw1.emp_data distribute by Ooid,Age)""".stripMargin

    val pattern         = "(?s)(?i)^.*?(select.*)".r
    val pattern(match1) = select1
    println(match1)
    val pattern(match2) = select2
    println(match2)

  }

}
