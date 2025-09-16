package com.adp.datacloud.ds.rdbms

import org.apache.log4j.Logger

import scala.xml.XML

case class DeltaConfiguration(
  hiveDbName: String,
  tgtSchemaName: String,
  rdbmsTableName: String,
  hiveSourceTableName: String,
  hiveTgtTableName: String,
  excludedColumnName: List[String] = List(),
  pkColumnNames: List[String] = List())


case class XmlInputProcessor (val xmlContent: String) { 
  
  private val logger = Logger.getLogger(getClass())
  
  val root = XML.loadString(xmlContent)
  
  require( (root \ "table").size > 0,
      "Atleast one Table need to be passed in XML config file for generating table statistics")
  

  def getDeltaDetailsFromXml(): List[DeltaConfiguration] = {
    (root \ "table").map(x => {
      val hiveDbName = (x \ "dbname").text.toLowerCase().trim()
      val tgtSchemaName = (x \ "targetschemaname").text.toLowerCase().trim()
      val rdbmsTableName = (x \ "rdbmstablename").text.toLowerCase().trim()
	  val hiveTgtTableName = (x \ "hivetargettablename").text.toLowerCase().trim()
      val hiveSourceTableName = if ((x \ "hivesourcetablename").isEmpty) (hiveTgtTableName + "_lake") else (x \ "hivesourcetablename").text.toLowerCase().trim()
      val excludedColumnName = ((x \ "excludedcolumn").map { a => a.text }).toList
      val pkColumnName = ((x \ "columns").filter { x => x.attribute("type").isDefined && x.attribute("type").get(0).text == "PK" } map { a => a.text }).toList

      DeltaConfiguration(hiveDbName, tgtSchemaName, rdbmsTableName,hiveSourceTableName,hiveTgtTableName, excludedColumnName, pkColumnName)
    }).toList
  }  
  
}