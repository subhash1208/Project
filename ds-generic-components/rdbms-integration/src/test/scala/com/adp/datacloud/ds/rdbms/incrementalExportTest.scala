package com.adp.datacloud.ds.rdbms

// TODO: Change this to use scalatest
object incrementalExportTest {

  def main(args: Array[String]) {
    testMergeScriptGeneration()
  }

  def testMergeScriptGeneration() {

    println(
      incrementalDataExporter.generateMergeScript(
        "mytable",
        "myschema",
        List("pk1", "pk2"),
        List("pk1", "pk2", "col1", "col2", "col3"),
        true,
        "postgres"))

  }

}
