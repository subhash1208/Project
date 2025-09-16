package com.adp.datacloud.ds

import com.adp.datacloud.cli.hierarchyOptions
import com.adp.datacloud.ds.orgStructure.buildHierarchy
import com.adp.datacloud.util.SparkTestWrapper
import org.apache.log4j.Logger
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class orgStructureTest extends SparkTestWrapper with Matchers {

  override def appName: String = getClass.getCanonicalName

  private val logger = Logger.getLogger(getClass())

  override def beforeAll(): Unit = {
    super.beforeAll()
    cleanAndSetupDatabase("testgreenmain")
    createTableFromResource("testgreenmain", s"layers_test_input", s"layers_test_input")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    displayAllTables("testgreenmain")
  }

  lazy val layersConfig = hierarchyOptions.parse(getConfigArgs)

  private def getConfigArgs() = {

    val inputSql     = s"hierarchy_input.sql"
    val inputSqlPath = getPathByResourceName(inputSql)
    sparkSession.sparkContext.addFile(inputSqlPath)

    getListOfFiles(SparkFiles.getRootDirectory()).foreach(path =>
      println("SPARK_FILE_PATH: " + path))

    // --num-shuffle-partitions 400
    val argsString = s"""
        --input-spec $inputSql 
        --application-name connect-by
        --hiveconf __GREEN_MAIN_DB__=testgreenmain
        --output-db-name testgreenmain  
        --connect-by-column pers_obj_id  
        --connect-by-parent mngr_pers_obj_id  
        --hierarchy-table tmp_hrchy_table  
        --hierarchy-exploded-table tmp_hrchy_table_exploded  
        --group-columns clnt_obj_id
      """
    val configArgs = argsString.trim.stripMargin.split("\\s+")
    configArgs
  }

  test("test build hierarchy") {
    buildHierarchy(layersConfig)
    val hierarchyExplodedTable =
      sparkSession
        .table("testgreenmain.tmp_hrchy_table_exploded")

    assert(!hierarchyExplodedTable.isEmpty, "hierarchyExplodedTable cannot be empty")

    hierarchyExplodedTable
      .filter("layer <= 3")
      .orderBy("pers_obj_id")
      .show(200)

    val hierarchyTable = sparkSession.table("testgreenmain.tmp_hrchy_table").sort("layer")
    assert(!hierarchyTable.isEmpty, "hierarchyTable cannot be empty")
    hierarchyTable.show(100)
  }

}
