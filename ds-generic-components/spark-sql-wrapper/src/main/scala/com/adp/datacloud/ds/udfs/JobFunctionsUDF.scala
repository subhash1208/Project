package com.adp.datacloud.ds.udfs

import org.apache.spark.sql.SparkSession
import scala.collection.immutable.ListMap
import java.util.regex.Pattern

/**
 * This UDF uses a simple rule based system to reasonably determine the job function given inputs 
 */

// Functions are of two types. 
// Industry Specific Functions
    // Consulting
    // Engineering
    // Operations
    // Program & Product Management
    // Quality Assurance
    // Research & Development
    // Sales
// Generic Functions
    // Finance & Accounting
    // Administrative
    // Arts and Design
    // Business Development
    // Community & Social Services
    // Education
    // Healthcare Services
    // Human Resources
    // Information Technology
    // Legal
    // Marketing
    // Media & Communications
    // Military & Protective Services
    // Purchasing
    // Real Estate
    // Customer Service / Support

object JobFunctionsUDF {
  
  val functionPatterns = ListMap(
      // Function -> (whitelist_words, blacklist_words, regex)
      // The order of the below is important.
      
      // Generic Functions
      "Finance & Accounting" -> (List("accounting","acctng","finance"), List[String](), false),
      "Administrative Support" -> (List("admin"), List("network", "systems"), false),
      "Community & Social Services" -> (List("community","social"), List("banking"), false),
      "Education" -> (List("education"), List[String](), false),
      "Creative, Arts and Graphics Design" -> (List("arts","graphics","creative"), List("parts"), false),
      "Business Development" -> (List("business%development"), List[String](), false),
      "Healthcare" -> (List("health","hospital"), List("education","community","hospitality"), false),
      "Human Resources" -> (List("hr "," hr","hr-","-hr","human%resourc","staffing","recruit"), List[String]("hrs"), false),
      "Information Technology" -> (List("^it[\\-\\s].*", "[\\-\\s]it$","^it$"), List[String](), true),
      "Legal" -> (List("legal", "counsel"), List[String]("counselor","counseling"), false),
      "Media & Communications" -> (List("media"), List("intermediate"), false),
      "Military & Protective Services" -> (List("military","protecti"), List[String](), false),
      "Purchasing & Procurement" -> (List("purchas","procure"), List[String](), false),
      "Real Estate" -> (List("real%estate","workplace"), List[String](), false),
      "Customer Service & Support" -> (List("cust%svc","cust%support","cust%service","client%svc","client%support","client%service"), List[String](), false),
      
      // Industry Functions
      "Sales & Marketing" -> (List("sales", "marketing", "mktng", "mrktng"), List[String](), false),
      "Consulting" -> (List("consulting"), List[String](), false),
      "Quality Assurance" -> (List("^qa[\\-\\s].*", "[\\-\\s]qa$","^qa$", ".*quality.*"), List[String](), false),
      "Engineering" -> (List("engineer"), List[String](), false),
      "Operations" -> (List("operations", "production", "manufactur"), List[String](), false),
      "Product & Project Management" -> (List(".*(product|project|program)[\\-\\s](manage|mngmt).*"), List[String](), true),
      "Research & Development" -> (List("r&d","research"), List[String](), false)
  )
  
  val listOfFunctionChecks = functionPatterns.map({ x => {
      if (!x._2._3) {
        // Perform simple match. Under the hood, this still uses regex
        val matchPatterns = x._2._1.map( y => (".*" + y.replace("%",".*") + ".*"))
        val nonMatchPatterns = x._2._2.map( y => (".*" + y.replace("%",".*") + ".*"))
        (input: String) => if (input != null && matchPatterns.exists({pattern => input.matches(pattern)}) && !nonMatchPatterns.exists({pattern => input.matches(pattern)})) {
          Some(x._1)
        } else None
      } else {
        (input: String) => if (input != null && x._2._1.exists(pattern => input.matches(pattern)) && !x._2._2.exists(pattern => input.matches(pattern))) {
          Some(x._1)
        } else None
      }
    }})
  
  val determineJobFunction = (hr_job_func_dsc: String, hr_orgn_shrt_dsc: String, hr_bus_unit_dsc: String) => {
    // When available hr_job_func_dsc shall be used to determine the function
    // If no function resolution can be made from this field, we attempt to determine it using a combination of orgn and business unit
    // If the above logic yields a Generic function, then we leave it as-is.
    // If it instead leads to an industry-specific function, we will try to disambiguate it using sector.
    
    val job_func = if (hr_job_func_dsc != null) hr_job_func_dsc.toLowerCase() else ""
    val orgn = if (hr_orgn_shrt_dsc != null) hr_orgn_shrt_dsc.toLowerCase() else ""
    val bu = if (hr_bus_unit_dsc != null) hr_bus_unit_dsc.toLowerCase() else ""
    
    val firstMatchingFunction = (
        listOfFunctionChecks.filter(func => func(job_func).isDefined) ++
        listOfFunctionChecks.filter(func => func(orgn + bu).isDefined)
        ).headOption
    firstMatchingFunction match {
      case Some(func) => {
        func(job_func) match {
          case Some(x) => x
          case None => func(orgn + bu).get
        }
      }
      case None => ""
    }
  }
  
  def registerUdfs(implicit sparkSession: SparkSession) = {
    sparkSession.udf.register("get_job_function", determineJobFunction)
  }
  
  def main(args: Array[String]) {
    println(determineJobFunction("some quality assu", null, null))
    println(determineJobFunction("texas", null, null))
    println(determineJobFunction("Parts", null, null))
    println(determineJobFunction(null, "Semiconductor Machinery Manufacturing", null))
    println(determineJobFunction("paralegal", null, null))
    println(determineJobFunction("Manufacturing", null, "Medical and hospital equipment"))
    println(determineJobFunction("Finance and Accounting", null, "Aircraft Engine & Engine Parts"))
    println(determineJobFunction("Human Resources", null, "Designs, manufactures, markets and"))
  }
  
}