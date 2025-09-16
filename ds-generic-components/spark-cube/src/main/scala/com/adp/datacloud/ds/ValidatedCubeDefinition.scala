package com.adp.datacloud.ds

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.xml.XML

/**
 * Validator for cube configuration
 */
protected case class ValidatedCubeDefinition(xmlConfigString: String) {

  private val logger = Logger.getLogger(getClass())

  val rootElement = XML.loadString(xmlConfigString)
  val cubeConfig  = (rootElement \ "cube").head

  val SIMPLE_AGG_TYPES = List[String](
    "SUM",
    "MAX",
    "MIN",
    "PERCENT",
    "AVG",
    "STDDEV",
    "COUNT",
    "APPROX_COUNT_DISTINCT",
    "COUNT_DISTINCT",
    "COUNT_DISTINCT_PARTITIONED",
    "NUM_MISSING",
    "NUM_NON_MISSING",
    "COLLECT_SET")
  val ARRAY_AGG_TYPES =
    List[String]("HIVE_PERCENTILE", "PERCENTILE_APPROX", "PERCENTILE_OF_PERCENTAGES")
  val AGG_TYPES = SIMPLE_AGG_TYPES ++ ARRAY_AGG_TYPES

  // Two level aggregations
  val COMPLEX_AGG_TYPES = List("PERCENTILE_OF_PERCENTAGES", "COUNT_DISTINCT_PARTITIONED")

  /**
   * A Whole bunch of checks to ensure that the XML configuration provided for cube builds is consistent
   */

  require(
    (rootElement \ "cube").size == 1,
    "Exactly one cube is allowed per configuration file. (This may be enhanced in future)")

  require(
    (rootElement \ "cube").forall(x =>
      !(x \ "dimensions" \\ "dimension").isEmpty || !(x \ "groupingsets" \\ "groupingset").isEmpty),
    "Either of (and not both of) <dimensions> or <groupingsets> should be defined for building cubes.")

  require(
    (rootElement \ "cube").forall(x =>
      !(!(x \ "dimensions" \\ "dimension").isEmpty && !(x \ "groupingsets" \\ "groupingset").isEmpty)),
    "Both <dimensions> or <groupingsets> cannot be defined. Use only one of them. ")

  require(
    (rootElement \ "cube").forall(x => !(x \ "facts" \ "fact").isEmpty),
    "Facts list cannot be empty")

  require(
    (rootElement \ "cube").forall({ y =>
      if ((y \ "type").text == "PARTITIONED") {
        !(y \ "partitionspec" \\ "dimension").isEmpty
      } else true
    }),
    "Partitioned Cube requires a non-empty partition spec to be provided")

  require(
    (rootElement \ "cube").forall({ y =>
      (y \\ "dimension")
        .map { x => x.text }
        .toSet
        .intersect((y \\ "fact" \ "name").map { x => x.text }.toSet)
        .isEmpty
    }),
    "Column Conflict: There exists a dimension and measure with the same name in your configuraion.")

  require(
    (rootElement \ "cube").forall({ y =>
      if ((y \ "type").text != "PARTITIONED") {
        (y \ "partitionspec" \\ "dimension").isEmpty
      } else true
    }),
    "Partition Spec must not be provided for a non-partitioned cube.")

  require(
    (rootElement \ "cube").forall({ y =>
      if ((y \ "type").text == "PARTITIONED") {
        (y \ "facts" \ "fact").forall({ x =>
          COMPLEX_AGG_TYPES.contains((x \ "aggregation" \ "type").text)
        })
      } else true
    }),
    "Partitioned Cube Definition cannot contain 1-level aggregations. Currently, the 2-level aggregations supported are " + COMPLEX_AGG_TYPES
      .mkString(","))

  // TODO:
  /*
   * 1. Validate that no duplicate fact names exist
   * 3. Validate that no duplicate dimensions exist
   * 2. Validate that depth value is not wrong
   */

  require(
    (rootElement \\ "fact").forall { x =>
      if (COMPLEX_AGG_TYPES.contains(
          (x \\ "type").text) && "COUNT_DISTINCT_PARTITIONED" != (x \\ "type").text) {
        !(x \\ "argument").isEmpty
      } else true
    },
    "Missing mandatory arguments for one or more 2-level aggregations. Please check your configuration")

  require(
    (rootElement \ "cube").forall { y =>
      if ((y \ "type").text != "PARTITIONED") {
        (y \ "facts" \ "fact").forall({ x =>
          !COMPLEX_AGG_TYPES.contains((x \ "aggregation" \ "type").text)
        })
      } else true
    },
    "Non Partitioned Cubes Cannot contain complex 2-level aggregations.")

  require(
    (rootElement \ "cube" \ "facts" \ "fact").forall({ x =>
      !(x \ "aggregation" \ "type").isEmpty &&
      AGG_TYPES.contains((x \ "aggregation" \ "type").text)
    }),
    "Invalid Aggregation type(s) detected in configuration. Please check the aggregations!")

  require(
    (rootElement \ "cube" \ "facts" \ "fact").forall({ x =>
      !((x \ "column").text.isEmpty && (x \ "expr").text.isEmpty) || ((x \ "aggregation" \ "type").text == "COUNT")
    }),
    "Fact Column/Expr cannot be empty when aggregation type is not COUNT")

  require(
    (rootElement \ "cube" \ "facts" \ "fact").forall({ x =>
      !(x \ "name").text.isEmpty
    }),
    "Fact Name cannot be empty")

  val factNames = (rootElement \ "cube" \ "facts" \ "fact").map { x => (x \ "name").text }
  val duplicateFacts =
    factNames.groupBy(identity).collect { case (x, ys) if ys.lengthCompare(1) > 0 => x }

  require(
    duplicateFacts.isEmpty,
    "Duplicate fact names exist. Please fix them in your configuration - " + duplicateFacts
      .mkString(","))

  require(
    (rootElement \ "cube" \ "facts" \ "fact").forall({ x =>
      if (List("HIVE_PERCENTILE", "PERCENTILE_OF_PERCENTAGES", "PERCENTILE_APPROX")
          .contains((x \ "aggregation" \ "type").text)) {
        (x \ "aggregation" \ "arguments" \ "argument").size >= 1 &&
        (x \ "aggregation" \ "arguments" \ "argument")(0).text.split(",").forall { y =>
          y.toInt > 0 && y.toInt < 100
        }
      } else true
    }),
    "PERCENTILE type measures expect the first argument to be the list of percentile values desired - in the range (0,100)")

  require(
    (rootElement \ "cube" \ "facts" \ "fact").forall({ x =>
      if (List("PERCENT", "PERCENTILE_OF_PERCENTAGES").contains(
          (x \ "aggregation" \ "type").text)) {
        (x \ "matches").size == 1 &&
        !(x \ "matches").text.isEmpty
      } else true
    }),
    "PERCENT type measures expect a regex match criteria to be specified using the 'matches' tag")

  /**
   * Scans dimensions and returns any possible warnings.
   * Returns a list of CubeDimensionSummary objects for each cube in the configuration
   *
   */
  def scanDimensions(inputDataFrame: DataFrame): List[CubeDimensionSummary] = {

    val dataFrame = inputDataFrame.na.fill(
      "UNKNOWN",
      ((rootElement \\ "dimension") ++ (rootElement \\ "pivot")).map { x => x.text })

    (rootElement \ "cube").map { cubeConfig =>
      {
        val hierarchicalDimensions = (cubeConfig \ "dimensions" \ "hierarchy").map { x =>
          (x \ "dimension").map { _.text }.mkString(",")
        }
        val dimensions =
          ((cubeConfig \ "dimensions" \ "dimension") ++ (cubeConfig \ "pivots" \ "pivot"))
            .map { _.text }

        val dimensionSummary = (hierarchicalDimensions ++ dimensions).map { x =>
          val cardinality =
            dataFrame.select(x.split(",").map { y => col(y) }: _*).distinct().count()
          val dataTypes = x
            .split(",")
            .map { y => dataFrame.schema.find { p => p.name == y } }
            .flatten
            .mkString(",")
          DimensionSummary(x, dataTypes, cardinality)
        }

        val potentialCubeSize = dimensionSummary.foldLeft(1L) { (a, b) =>
          a * (b.cardinality + 1)
        }

        val warnings = dimensionSummary.map { x =>
          if ((x.dataTypes.contains("IntegerType") || x.dataTypes.contains(
              "LongType") || x.dataTypes.contains("DoubleType")) && x.cardinality > 200) {
            Some(
              "There exists numeric types in dimensions with cardinality of more than 200. This is not typical. Are you sure this is what you want ???")
          } else if (x.cardinality > 1500) {
            Some(
              "There exists a dimension (simple or hierarchical) with a cardinality of more than 1500. This is not typical. Are you sure this is what you want ???")
          } else
            None
        } ++ {
          if (potentialCubeSize > 200000000000L)
            List(Some(
              "Risk of too-high cardinality (greater than 200 billion)... Potential cube size = " + potentialCubeSize))
          else
            List()
        }

        CubeDimensionSummary(warnings.flatten, dimensionSummary)

      }

    } toList

  }

}
