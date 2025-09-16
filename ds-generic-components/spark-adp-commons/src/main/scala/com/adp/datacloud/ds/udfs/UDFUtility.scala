package com.adp.datacloud.ds.udfs

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.SparkSession
import java.security.MessageDigest
import java.math.BigInteger

import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder
import com.amazonaws.services.comprehend.model.DetectDominantLanguageRequest

import scala.io.Source
import org.apache.spark.sql.Row
import org.apache.commons.lang3.StringEscapeUtils

import scala.xml.XML
import scala.util.Try
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * @author SagarSid
 *
 * registers custom functions as udfs in the current sparkSession to be available in spark sql
 *
 */
object UDFUtility {

  case class Position (
      title: String,
      position_type: String,
      company_name: String,
      start_date: String,
      end_date: String,
      description: String
  )

  case class Education (
      school_type: String,
      school_name: String,
      degree_type: String,
      degree_name: String,
      degree_major: String,
      start_date: String,
      end_date: String
  )

  case class Competency (
      competency_name: String,
      competency_group: String,
      total_experience: Int
  )

  val get_qualification_summary = (resumeParsedXMLString: String) => {
    try {
      val resumeXMLNode = try {
          // remove whitespace at the beginning of the xml string
          XML.loadString(resumeParsedXMLString.trim().replaceFirst("^([\\W]+)    <", "<"))
      } catch {
          // incase if string needs xml unescape
          case e: Exception => XML.loadString(StringEscapeUtils.unescapeXml(resumeParsedXMLString).trim().replaceFirst("^([\\W]+)<", "<"))
      }
      (resumeXMLNode \\ "Qualifications" \ "QualificationSummary").text
    } catch {
      case e: Exception => null.asInstanceOf[String]
    }
  }

  val get_skills = (resumeParsedXMLString: String) => {
    try {
      val resumeXMLNode = try {
          // remove whitespace at the beginning of the xml string
          XML.loadString(resumeParsedXMLString.trim().replaceFirst("^([\\W]+)    <", "<"))
      } catch {
          // incase if string needs xml unescape
          case e: Exception => XML.loadString(StringEscapeUtils.unescapeXml(resumeParsedXMLString).trim().replaceFirst("^([\\W]+)<", "<"))
      }
      val competencies = (resumeXMLNode \\ "Qualifications" \ "Competency")
      if (competencies.size == 0)
        null.asInstanceOf[List[Competency]]
      else
        competencies.map({ x =>
            Competency(
                (x \ "@name").text,
                (x \\ "StringValue" filter { _ \\ "@description" exists (_.text == "taxonomies") }).text,
                Try((x \\ "NumericValue" filter { _ \\ "@description" exists (_.text == "Total Months") }).text.toInt).toOption.getOrElse(0)
            )
        })
    } catch {
      case e: Exception => null.asInstanceOf[List[Competency]]
    }
  }

  val get_licenses = (resumeParsedXMLString: String) => {
    try {
      val resumeXMLNode = try {
          // remove whitespace at the beginning of the xml string
          XML.loadString(resumeParsedXMLString.trim().replaceFirst("^([\\W]+)<", "<"))
      } catch {
          // incase if string needs xml unescape
          case e: Exception => XML.loadString(StringEscapeUtils.unescapeXml(resumeParsedXMLString).trim().replaceFirst("^([\\W]+)<", "<"))
      }
      val licenses = (resumeXMLNode \\ "LicenseOrCertification")
      if(licenses.size == 0)
        null.asInstanceOf[List[String]]
      else
        licenses.map({ x => (x \\ "Name").text })
    } catch {
      case e: Exception => null.asInstanceOf[List[String]]
    }
  }

  val get_education_history = (resumeParsedXMLString: String) => {
    try {
      val resumeXMLNode = try {
          // remove whitespace at the beginning of the xml string
          XML.loadString(resumeParsedXMLString.trim().replaceFirst("^([\\W]+)    <", "<"))
      } catch {
          // incase if string needs xml unescape
          case e: Exception => XML.loadString(StringEscapeUtils.unescapeXml(resumeParsedXMLString).trim().replaceFirst("^([\\W]+)<", "<"))
      }
      val educationHistory = (resumeXMLNode \\ "SchoolOrInstitution")
      if(educationHistory.size == 0)
        null.asInstanceOf[List[Education]]
      else
        educationHistory.map({x =>
            Education(
                (x \ "@schoolType").text,
                (x \\ "SchoolName").text,
                (x \\ "@degreeType").text,
                (x \\ "DegreeName").text,
                (x \\ "DegreeMajor" \ "Name").text,
                (x \\ "StartDate" \ "AnyDate").text,
                (x \\ "EndDate" \ "AnyDate").text
            )
        })
    } catch {
      case e: Exception => null.asInstanceOf[List[Education]]
    }
  }

  val get_employment_history = (resumeParsedXMLString: String) => {
    try {
      val resumeXMLNode = try {
          // remove whitespace at the beginning of the xml string
          XML.loadString(resumeParsedXMLString.trim().replaceFirst("^([\\W]+)    <", "<"))
      } catch {
          // incase if string needs xml unescape
          case e: Exception => XML.loadString(StringEscapeUtils.unescapeXml(resumeParsedXMLString).trim().replaceFirst("^([\\W]+)<", "<"))
      }
      val positionHistory = (resumeXMLNode \\ "PositionHistory")
      if(positionHistory.size == 0)
        null.asInstanceOf[List[Position]]
      else
        positionHistory.map({x =>
            Position(
                (x \\ "Title").text,
                (x \ "@positionType").text,
                (x \\ "OrganizationName").text,
                (x \\ "StartDate" \ "AnyDate").text,
                (x \\ "EndDate" \ "AnyDate").text,
                (x \\ "Description").text
            )
        })
    } catch {
      case e: Exception => null.asInstanceOf[List[Position]]
    }
  }

  val validStates = scala.Array("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO",
    "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY")

  val xmlEscape = (xmlString: String) => {
    scala.xml.Utility.escape(xmlString)
  }
  val xmlUnEscape = (xmlString: String) => {
    StringEscapeUtils.unescapeXml(xmlString)
  }

  val acronymMap = {
    val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("acronyms.txt"))
      .getLines().withFilter(_.trim.nonEmpty)

    lines.map { line =>
      val Array(acronym, expansions) = line.split("\\t").take(2).map(_.trim)
      acronym.toLowerCase -> expansions.split(",").map(x=>x.replace("\"", "")).head.trim
    }.toMap
  }

  val strSanitize = (value: String) => {
    Option(value).map { value =>
      val words = value.split("[^a-zA-Z]+") // Split on special characters
      val cleanedWords = words.flatMap { word =>
        val cleanedWord = word.replaceAll("\\b(?i)((x{0,3})(ix|iv|v?i{0,3}))\\b", "")
        acronymMap.getOrElse(cleanedWord.toLowerCase.trim, cleanedWord).split("\\s+")
      }.filter(_.length != 1)

      cleanedWords.filter(x => !x.matches("^[0-9]+$")).mkString(" ").trim
    }
  }

  val md5Hashing = (str: String) => {
    val digest = MessageDigest.getInstance("MD5")
      .digest(String.join("", str)
        .getBytes)
    val bigInt = new BigInteger(1, digest)
    val code = bigInt.toString(16)
    0.toString * (32 - code.length()) + code

  }

  val deriveUdiState = (state_udi: Map[String, Seq[Row]]) => {

    try{
        val filteredUdi = state_udi.filterKeys(x => validStates.contains(x))
          .map(k => (k._1, k._2.map(x => x.getDouble(2)), k._2.map(_.getString(0)), k._2.map(_.getString(1))))
          .map(x => (x._1, (x._3, x._4, x._2).zipped.toSeq))
          .map(p => (p._1, p._2.filter(x => (x._1.equals("ee") || x._1.equals("er")) && (x._2.equals("sui") || x._2.equals("sui_tb")))))
          .filter(x => !x._2.isEmpty)

        if (!filteredUdi.isEmpty) {
          val maxVals = filteredUdi.map(x => (x._1, x._2.map(_._3)))
            .toList
            .map(x => (x._1, x._2.sorted.reverse.head))

          val maxAmt = maxVals.sortBy(f => f._2).reverse.head
          maxAmt._1  }
        else null
    }

    catch {
      case e: Exception => {
        println(s"Handled Exception while processing state_udi : ${state_udi}")
        println(e)
        null
      }
    }
  }

  val deriveTaxState = (state_taxes: Map[String, Double]) => {

    try{

        val filteredTax = state_taxes.filterKeys(x => validStates.contains(x))

        if (!filteredTax.isEmpty) {
          val maxAmt = filteredTax.valuesIterator.max
          filteredTax.filter(p => p._2 >= maxAmt).keys.head}
        else null }
    catch{
      case e: Exception => {
        println(s"Handled exception while processing state_taxes : ${state_taxes}")
        println(e)
        null
      }
    }
  }

  case class Language(lang_cd: String, score: Float)

  val detectLanguages: String => mutable.Buffer[Language] = (text: String) => {
    Option(text).map { text =>
      val comprehendClient = AmazonComprehendClientBuilder
        .standard()
        .build()
      val detectLangReq = new DetectDominantLanguageRequest()
        .withText(text)
      comprehendClient.detectDominantLanguage(detectLangReq)
        .getLanguages
        .map(lang => Language(lang.getLanguageCode, lang.getScore))
        .sortBy(lang => -lang.score)
    }.getOrElse(mutable.Buffer[Language]())
  }

  def registerUdfs(implicit sparkSession: SparkSession) = {
    sparkSession.udf.register("xml_escape", xmlEscape)
    sparkSession.udf.register("xml_unescape", xmlUnEscape)
    sparkSession.udf.register("strSanitize", strSanitize)
    sparkSession.udf.register("md5_udf", md5Hashing)
    sparkSession.udf.register("derive_udistate", deriveUdiState)
    sparkSession.udf.register("derive_taxstate", deriveTaxState)

    sparkSession.udf.register("get_qualification_summary", get_qualification_summary)
    sparkSession.udf.register("get_skills", get_skills)
    sparkSession.udf.register("get_licenses", get_licenses)
    sparkSession.udf.register("get_education_history", get_education_history)
    sparkSession.udf.register("get_employment_history", get_employment_history)
    sparkSession.udf.register("detect_languages", detectLanguages)
  }
}