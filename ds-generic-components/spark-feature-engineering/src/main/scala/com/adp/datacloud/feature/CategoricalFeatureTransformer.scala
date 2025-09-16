package com.adp.datacloud.feature

import com.adp.datacloud.ml.WorkflowStep
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.xml.{Elem, XML}

/**
 * Ensures that only known levels of certain categorical
 * variables are maintained in the dataframe.
 *
 */
class CategoricalFeatureTransformer(
    val spark: SparkSession,
    xmlFilePath: String = "/appconfig/listofvalues.xml")
    extends WorkflowStep {

  private val rootElement   = XML.load(getClass.getResourceAsStream(xmlFilePath))
  private val COLUMN_SUFFIX = "_transformed"

  override def needsCheckpoint = false

  def translate(rootConfig: Elem, featureName: String) =
    udf((input: String) => {

      val featureRoot = (rootConfig \\ "features" \\ "feature").filter(x =>
        (x \ "@name").text == featureName)(0)

      val matches = ((featureRoot \ "levels" \ "level")
        .map(y => {
          if ((y \ "match").exists { x =>
              input != null && !("(?i)" + x.text).r.findFirstIn(input).isEmpty
            }) {
            Some((y \ "@name").text)
          } else None
        }))
        .flatten

      val default = (featureRoot \ "@default").headOption

      if (!matches.isEmpty) {
        Some(matches(0))
      } else if (matches.isEmpty && default.isDefined) {
        Some(default.get.text)
      } else {
        None
      }

    })

  /**
   * Apply the transformations as per the configurations defined in XML file
   */
  def run(df: DataFrame): DataFrame = {

    val categoricalFeatures = (rootElement \\ "features" \\ "feature")
      .map(x => (x \ "@name").text)
      .toSet
      .intersect(df.columns.toSet)

    categoricalFeatures.foldLeft(df)((frame, x) => {
      frame
        .withColumn(x + COLUMN_SUFFIX, translate(rootElement, x)(col(x)))
        .drop(x)
        .withColumnRenamed(x + COLUMN_SUFFIX, x)
    })

  }
}

object CategoricalFeatureTransformer {

  def main(args: Array[String]) {

    lazy val sparkSession = SparkSession
      .builder()
      .appName("SparkFeatureEngineering")
      .master("local")
      .getOrCreate()
    lazy val sc = sparkSession.sparkContext

    val q = sparkSession.createDataFrame(
      sc.parallelize(
          List(
            "not married",
            "non-marRIEd partner",
            "Widowed/Viudo",
            "Married",
            null,
            "un-married"))
        .map { x => Row(x) },
      StructType(List(StructField("martl_stus_dsc", StringType, nullable = true))))
    q.show()
    val p  = new CategoricalFeatureTransformer(sparkSession)
    val q1 = p.run(q)("martl_stus_dsc")
    val q2 = p.run(q)
    q2.show()
    println("done")
  }

}
