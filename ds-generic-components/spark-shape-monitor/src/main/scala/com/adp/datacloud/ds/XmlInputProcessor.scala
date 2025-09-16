package com.adp.datacloud.ds

import org.apache.log4j.Logger

import scala.xml.XML

case class ShapeConfiguration (
    dbName            :   String,
    tableName         :   String,
    partitionColumns  :   List[String] = List(),
    excludeColumns    :   List[String] = List(),
    analyzeColumns    :   List[String] = List(),
    virtualColumns    :   List[(String, String)] = List(),
    uniqueColumns     :   List[String] = List(),           //To apply Unique key validation
    columnValidations :   List[ShapeValidation] = List()
)

case class ShapeValidation (
    columnName        : String,
    validationExpr    : Option[String] = None , // Row level validations for columns
    maxNullPctg       : Option[String] = None,  // Aggregation level validation
    maxCardinality    : Option[String] = None,  // Aggregation level validation
    minCardinality    : Option[String] = None,  // Aggregation level validation
    deseasonDetrendMetrics   : List[(String, Option[Double], Option[Double])]   = List() //Applying Deseason and validate
)

case class XmlInputProcessor (val xmlContent: String) { 
  
  private val logger = Logger.getLogger(getClass())
  
  val root = XML.loadString(xmlContent)
  
  require( (root \ "table").size > 0,
      "Atleast one Table need to be passed in XML config file for generating table statistics")
  
  require( (root \ "table" ).forall( x => x.attribute("dbname").isDefined ),
      "DBname is a mandatory argument and it should be provided in XML config file")
      
  require( (root \ "table" ).forall( x => x.attribute("tablename").isDefined ),
      "Tablename is a mandatory argument and it should be provided in XML config file")
      
  val tablenames = (root \ "table" \ "tablename").map(_.text.trim()).toList
  
  def getValidationExpressions (column: scala.xml.NodeSeq) : List[ShapeValidation] ={
    
    val validations = column.map(x =>( x.attribute("name").get(0).text.toLowerCase().trim,
                                       if (x.attribute("valid_values").isDefined) Some(x.attribute("valid_values").get(0).text.trim()) else None,
                                       if (x.attribute("nullable").isDefined) Some(x.attribute("nullable").get(0).text.toLowerCase().trim()) else None,
                                       if (x.attribute("max_null_pctg").isDefined) Some(x.attribute("max_null_pctg").get(0).text.toLowerCase().trim()) else None,
                                       if (x.attribute("max_cardinality").isDefined) Some(x.attribute("max_cardinality").get(0).text.toLowerCase().trim()) else None,
                                       if (x.attribute("min_cardinality").isDefined) Some(x.attribute("min_cardinality").get(0).text.toLowerCase().trim()) else None,
                                       if (x.attribute("max_value").isDefined) Some(x.attribute("max_value").get(0).text.toLowerCase().trim()) else None,
                                       if (x.attribute("min_value").isDefined) Some(x.attribute("min_value").get(0).text.toLowerCase().trim()) else None,
                                       if (x.attribute("simple_deseason_detrend_metrics").isDefined)
                                         x.attribute("simple_deseason_detrend_metrics").get(0).text.toUpperCase().trim().split(",").toList.map(_.split(":"))
                                         .map(x => if (x.length == 3) (x(0).trim(), Some(x(1).toDouble), Some(x(2).toDouble)) else (x(0).trim(), None, None) )
                                       else List()
                                       )) 
   
    val validationExpr = validations.map(x =>new ShapeValidation
                                              (x._1,
                                              Some(("case when 1=1 ") +
                                              (x._2 match {
                                                              case Some(m)                      => x._3 match { case Some(n) if n.equals("true") => s" and (${x._1} IN (${m}) or ${x._1} is null )"  // discrete value check with expected nulls 
                                                                                                                case _                           => s" and ${x._1} IN (${m}) " }    // discrete value check
                                                              case None                         =>  "" }) + 
                                              (x._3 match {
                                                              case Some(m) if m.equals("false") => s" and ${x._1} is not null "  // nullable false
                                                              case _                            =>  "" }) +
                                              (x._7 match {
                                                              case Some(m)                      => x._3 match { case Some(n) if n.equals("true") => s" and (${x._1} <= ${m} or ${x._1} is null )" // max value validations with expected nulls
                                                                                                                case _                           => s" and ${x._1} <= ${m} " }       // max value validatiom
                                                              case None                         =>  "" }) +
                                              (x._8 match{
                                                              case Some(m)                      => x._3 match { case Some(n) if n.equals("true") => s" and (${x._1} >= ${m} or ${x._1} is null )"  // min value validations with expected nulls
                                                                                                                case _                           => s" and ${x._1} >= ${m} " }       // min value validation 
                                                              case None                         =>  "" }) +
                                              ("then 0 else 1 end")) match {
                                                case Some("case when 1=1 then 0 else 1 end") => None
                                                case Some(expr)                              => Some(expr)
                                              },
                                              x._4 match{
                                                              case Some(m)                      => Some(s"case when NUM_MISSING / NUM_RECORDS > ${m}/100 then 'FAIL' else 'PASS' end")  //max percentage missing check
                                                              case None                         => None  }, 
                                              x._5 match{     
                                                              case Some(m)                      => Some(s"case when NUM_APPROX_UNIQUE_VALUES > ${m} then 'FAIL' else 'PASS' end")  //max cardinality check
                                                              case None                         => None  },
                                              x._6 match{
                                                             case Some(m)                      => Some(s"case when NUM_APPROX_UNIQUE_VALUES < ${m} then 'FAIL' else 'PASS' end")   //min cardinality check
                                                              case None                         => None  },
                                              x._9)
                                              ).toList
  validationExpr
  }
 
  def getShapeDetailsFromXml(): List[ShapeConfiguration] = {
    
    (root \ "table").map( x => {
      val dbName             = x.attribute("dbname").get(0).text.toLowerCase().trim()
      val tableName          = x.attribute("tablename").get(0).text.toLowerCase().trim()
      val partitionColumns   = if (!(x \ "partitioncolumns" ).text.trim.isEmpty()) (x \ "partitioncolumns" ).text.toLowerCase().trim.split(",").toList else List()
      val excludeColumns     = if (!(x \ "excludecolumns" ).text.trim.isEmpty())   (x \ "excludecolumns" ).text.toLowerCase().trim.split(",").toList   else List()
      val analyzeColumns     = if (!(x \ "analyzecolumns" ).text.trim.isEmpty())   (x \ "analyzecolumns" ).text.toLowerCase().trim.split(",").toList   else List()
      val virtualColumns     = if ((x \ "column").exists(_.attribute("expr").isDefined)) {
                                 (x \ "column" ).filter(_.attribute("expr").isDefined).map( vcolumn => (vcolumn.attribute("name").get(0).text, vcolumn.attribute("expr").get(0).text)).toList  
                               } else List()
      val uniqueColumns      = if (!(x \ "uniquecolumns" ).text.trim.isEmpty()) (x \ "uniquecolumns" ).text.toLowerCase().trim.split(",").toList else List()                         
      val columnValidations  = if ((x \ "column" ).size > 0) getValidationExpressions((x \ "column")) else List()
      ShapeConfiguration(dbName, tableName, partitionColumns, excludeColumns, analyzeColumns, virtualColumns, uniqueColumns, columnValidations)
    } 
    ).toList
    
  }  
  
}