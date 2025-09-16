package com.adp.datacloud.ds

import com.adp.datacloud.ds.udafs.BenHaimTomApproximatePercentileV2
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{array, avg, callUDF, coalesce, col, count, countDistinct, expr, lit, max, min, round, stddev_pop, sum, when, _}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}
import org.apache.spark.storage.StorageLevel
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write, writePretty}

import scala.collection.immutable
import scala.xml.Node

/**
 * Heart of the Cube Processing Framework
 */
class CubeProcessor(
    val xmlConfigString: String,
    val optionalTimeCube: Option[DataFrame] = None,
    val ignoreNullGroupings: Boolean        = true,
    val limitedTestMode: Boolean            = false) {

  private val logger = Logger.getLogger(getClass())

  // Decoupled validator.
  private val validator = ValidatedCubeDefinition(xmlConfigString)

  def scanDimensions(inputDataFrame: DataFrame): List[CubeDimensionSummary] = {
    validator.scanDimensions(inputDataFrame)
  }

  import validator._

  val partitionSpecColumns: List[String] = (cubeConfig \ "partitionspec" \\ "dimension").map { x =>
    x.text
  } toList
  val isPartitionedCube: Boolean = !partitionSpecColumns.isEmpty
  val groupingSets: List[Set[String]] = ((cubeConfig \ "groupingsets" \ "groupingset").map {
    _.text split (",") map { _.trim } toSet
  } toList)
  val dimensions: List[String] = ((cubeConfig \ "dimensions" \ "dimension").map { _.text } toList)
  // Note: The same dimension can be a part of multiple hierarchies.
  // Eg: Job-level can be a part of a hierarchy with ONET or ADP_LENS.
  val hierarchies: List[List[String]] = (cubeConfig \ "dimensions" \ "hierarchy").map { x =>
    (x \ "dimension").map { _.text } toList
  } toList

  // Deeper dimensions listed within hierarchies are excluded
  val baseDimensions: List[String] = (dimensions ++ (cubeConfig \ "dimensions" \ "hierarchy").map { x =>
    (x \ "dimension").head.text
  } toList) ++ (groupingSets.flatten.toSet.toList)

  val xorBlacklists: List[(String, Seq[String])] = ((cubeConfig \\ "dimension")
    .filter { x => x.attribute("xor-group-name").isDefined })
    .flatMap { x =>
      x.attribute("xor-group-name").get(0).text.split(" ").map({ y => y -> x.text })
    }
    .groupBy(_._1)
    .map({ case (k, v) => (k, v.map(_._2)) })
    .toList

  val limitedDepthDimensions: List[(String, Int)] = ((cubeConfig \\ "dimension")
    .filter { x => x.attribute("max-depth").isDefined })
    .map { x => x.text -> x.attribute("max-depth").get(0).text.toInt }
    .toList

  val mandatoryDimensions: List[String] = ((cubeConfig \\ "dimension")
    .filter { x =>
      x.attribute("mandatory").isDefined && x.attribute("mandatory").get(0).text.toBoolean
    })
    .map { x => x.text } toList

  /**
   * Utility method used while computing grouping sets combinations
   */
  private def rollups(xs: List[String]): List[List[String]] = {
    xs match {
      case List() => List(List[String]())
      case str :: tail => {
        val tailResult = rollups(tail)
        List(List[String]()) ++ tailResult.map { x => List(str) ++ x }
      }
    }
  }

  /**
   * Returns the columns necessary in the source frame for building this cube
   */
  private def baseColumns = {
    ((cubeConfig \\ "dimensions" \\ "dimension").map { x => x.text } ++
      (cubeConfig \\ "groupingsets" \\ "groupingset").flatMap { x =>
        x.text.split(",").map { _.trim }
      } ++
      (cubeConfig \\ "addons" \ "supportcolumns" \ "column").map { x => x.text } ++
      (cubeConfig \\ "facts" \\ "fact" \\ "column").map { x => x.text }).toSet.toList
  }

  /**
   * Returns the partition spec columns
   */
  def partitionColumns = {
    (cubeConfig \\ "partitionspec" \\ "dimension").map { x => x.text }
  }

  def postProcessCube(cubeConfig: Node, cube: DataFrame) = {

    val joinedDf =
      if (cubeConfig.text.contains("sys_calendar_days") && optionalTimeCube.nonEmpty) {
        // Note that in this case, the time hierarchy automatically becomes a pivot
        val joinColumns = cube.columns.intersect(optionalTimeCube.get.columns)
        val timeCube = Set("yr_cd", "qtr_cd", "mnth_cd", "wk_cd")
          .diff(joinColumns.toSet)
          .toSeq
          .foldLeft(optionalTimeCube.get)({ (df, y) => df.filter(col(y).isNull) })
        cube
          .join(timeCube)
          .filter((joinColumns map { y => cube(y) <=> timeCube(y) }) reduce { (a, b) =>
            a.and(b)
          })
          .select((cube.columns map { cube(_) }) :+ col("sys_calendar_days"): _*)
      } else
        cube

    val postprocessedDF =
      (cubeConfig \ "postprocess" \ "column").foldLeft(joinedDf)((df, col) => {
        val aggregateBounds = (col \ "inclusionrange").text.split(",") filter { x =>
          !x.isEmpty()
        }
        val spec = expr((col \ "expr")(0).text)
        val boundedSpec = if (aggregateBounds.isEmpty) {
          spec
        } else if (aggregateBounds.size == 1) {
          when(spec.geq(lit(aggregateBounds(0).toDouble)), spec)
        } else {
          when(
            spec
              .geq(lit(aggregateBounds(0).toDouble))
              .and(spec.leq(lit(aggregateBounds(1).toDouble))),
            spec)
        }
        df.withColumn((col \ "name")(0).text, boundedSpec)
      })

    val postprocessFilteredDF = (cubeConfig \ "postprocess" \ "filters" \ "filter")
      .map { x => x.text }
      .foldLeft(postprocessedDF)((df, filter) => {
        df.filter(filter)
      })

    if (ignoreNullGroupings) {
      val df =
        (dimensions ++ hierarchies.flatten.toSet.toList ++ groupingSets.flatten.toSet.toList)
          .foldLeft(postprocessFilteredDF) { (df, dim) =>
            df.filter(col(dim).isNull.or(col(dim).notEqual("__NULL__")))
          }
      df
    } else postprocessFilteredDF

  }

  /**
   * Processes one cube on a dataFrame
   */
  private def processCube(fullDataFrame: DataFrame) = {

    val filters  = (cubeConfig \ "filters" \ "filter").map { x => x.text }
    val cubeName = (cubeConfig \ "name").text
    val partitionSpecColumns = (cubeConfig \\ "partitionspec" \\ "dimension").map { x =>
      x.text
    }.toSet
    val isPartitionedCube = (cubeConfig \ "type").text == "PARTITIONED"
    val groupColumns = (cubeConfig \ "dimensions" \\ "dimension")
      .map { x => x.text }
      .toSet
      .toList // This expression ensures that partition dimensions are excluded
    val simpleAggregationFacts = (cubeConfig \ "facts" \ "fact")
      .filter({ fact =>
        SIMPLE_AGG_TYPES.contains(
          (fact \\ "type").text
        ) // Select only facts with single level aggregations
      })
      .map { x => (x \\ "name").text }
    val arrayAggregationFacts = (cubeConfig \ "facts" \ "fact").filter({ fact =>
      ARRAY_AGG_TYPES.contains(
        (fact \\ "type").text
      ) // Select only facts with single level aggregations
    })
    val excludeCols = (cubeConfig \\ "facts" \\ "fact" \\ "name")
      .filter { x =>
        x.attribute("exclude").isDefined && x.attribute("exclude").get(0).text == "true"
      }
      .map(x => x.text)

    val dataFrame = filters
      .foldRight(fullDataFrame)({ (x, y) =>
        y.filter(x)
      })
      .select(baseColumns.head, baseColumns.tail: _*)

    val aggregatedData = processGroupingSet(dataFrame)

    // If second level aggregations exist, process them
    val result = if (isPartitionedCube) {

      val twoLevelAggregationColumns = (cubeConfig \ "facts" \ "fact").filter({ fact =>
        COMPLEX_AGG_TYPES.contains(
          (fact \\ "type").text
        ) // Select only facts with two level aggregations
      })

      val twoLevelAggregationColumnSpecs = twoLevelAggregationColumns.map { x =>
        {
          val factName       = (x \ "name")(0).text
          val percentileArgs = (x \ "aggregation" \\ "argument")
          val percentiles =
            if (!percentileArgs.isEmpty) percentileArgs(0).text.split(",") map { x =>
              x.toInt
            }
            else null
          val secondLevelAggregation =
            if ((x \ "aggregation" \\ "type").text == "PERCENTILE_OF_PERCENTAGES") {
              val maxSampleSize =
                if (percentileArgs.length == 2) lit(percentileArgs(1).text.toInt)
                else lit(2000)
              callUDF(
                "percentile_approx",
                col(factName),
                array(percentiles.map { x => lit(x.toDouble / 100.0) }: _*),
                maxSampleSize
              ) // Returns a column of "Array" type
            } else {
              // COUNT_DISTINCT_PARTITIONED. count distinct is already done Within the partitions (in the first level), just do the sum
              sum(col(factName))
            }

          secondLevelAggregation.alias(factName)
        }
      }

      aggregatedData.persist(StorageLevel.MEMORY_AND_DISK)
      aggregatedData.rdd.checkpoint() // truncate lineage

      // Filter the result frame to contain only Non-Null values of partitionSpec columns
      val filteredFrame = partitionSpecColumns.foldRight(aggregatedData)((x, y) =>
        y.filter(col(x).isNotNull).filter(col(x) =!= lit("null")))

      // Return the second level aggregated frame
      filteredFrame
        .groupBy(groupColumns.head, groupColumns.tail: _*)
        .agg(twoLevelAggregationColumnSpecs.head, twoLevelAggregationColumnSpecs.tail: _*)

    } else { aggregatedData }

    // At this point, there may be a few array columns in the data. Convert the array columns in percentiles to standalone columns for saving
    val selections = (groupColumns ++ simpleAggregationFacts).map { x =>
      col(x)
    } ++ arrayAggregationFacts.flatMap { x =>
      {
        (x \ "aggregation" \\ "argument")(0).text.split(",").zipWithIndex map {
          case (y, index) =>
            (col((x \\ "name").text)
              .getItem(index))
              .alias((x \ "name")(0).text + "_" + y + "th_percentile")
        }
      }
    } ++ (((cubeConfig \ "groupingsets" \ "groupingset")
      .flatMap { _.text split (",") map { _.trim } } toSet)
      .map { p: String => col(p) } toList)

    // Apply any needed post processing
    val postprocessedDF = postProcessCube(cubeConfig, result.select(selections: _*))

    //exclude all those columns with exclude="true"
    val finalFields =
      (postprocessedDF.columns.filter(!excludeCols.contains(_))).map(col(_))

    val finalDF = postprocessedDF.select(finalFields: _*)

    // Return the final form
    finalDF.withColumn(
      "aggregation_depth",
      lit(
        (dimensions ++ hierarchies.map(_.head) ++ groupingSets.flatten.toSet.toList)
          .foldLeft(lit(0))({ (accumulated_depth, dim) =>
            accumulated_depth.plus(when(col(dim).isNotNull, lit(1)).otherwise(lit(0)))
          })))

  }

  /**
   * Processes one "Non-Partitioned" cube/groupingset on a dataFrame.
   * Reduces the groupingSets within the group as necessary. Typically, groupingSets are reduced for one of the the following reasons
   *
   * 1) Hierarchical dimensions are defined
   * 2) Mandatory Dimensions are defined
   * 3) Depth criterion is specified
   *
   */
  private def processGroupingSet(dataFrame: DataFrame) = {

    /**
     * Utility operation used while computing grouping sets combinations
     */
    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }

    val columnTypesMap = dataFrame.schema.map { x => x.name -> x.dataType } toMap

    // "Cube" function is an alias for grouping sets.
    // Notice the order of dimensions being passed into the "cube" call
    // This is done intentionally to allow for filtering by depth in logic further below
    val groupedData = dataFrame.cube(
      ((partitionSpecColumns
        ++ ((hierarchies
          .flatMap { _.tail })
          .toSet
          .toList) // Because the same dimension can belong to multiple hierarchies
        ++ ((groupingSets.flatten).toSet.toList)
        ++ dimensions
        ++ (hierarchies.map { _.head })).map { col(_) }) reverse: _*)

    val aggregatedFrame = generateFactSpecifications(groupedData, columnTypesMap)

    // Reduce groupingSets further by partitionSpec columns
    partitionSpecColumns.foldLeft(aggregatedFrame)((a, b) => a.filter(col(b).isNotNull))

  }

  /**
   * Core aggregation logic of facts using the given dimensions.
   */
  private def generateFactSpecifications(
      groupedData: RelationalGroupedDataset,
      columnTypesMap: Map[String, DataType]) = {

    val factSpecs = (cubeConfig \ "facts" \ "fact").foldLeft(List[Column]())((y, x) => {
      val excludeZeros = (x \ "aggregation" \ "excludezeros").text == "true"

      // Generate the column specification
      val baseColumnSpec =
        if ((x \ "expr").text != "") // Use expr if it exists
          expr((x \ "expr").text)
        else if ((x \ "matches").text != "") // Apply a "matches clause if it exists"
          when(
            expr(
              "cast(" + (x \ "column").text + " as string) rlike '" + (x \ "matches").text + "'"),
            1).otherwise(0)
        else
          col((x \ "column").text)

      // Apply any additional filter specifications "when" specified
      val columnSpec = if ((x \ "when").text != "") {
        when(expr((x \ "when").text), baseColumnSpec)
      } else baseColumnSpec

      // Apply the filters
      val column = if (excludeZeros) {
        when(columnSpec =!= lit(0), columnSpec)
      } else if ((x \ "aggregation" \ "inclusionrange").text != "") {
        val bounds = (x \ "aggregation" \ "inclusionrange").text.split(",")
        when(
          columnSpec >= lit(bounds(0).toDouble) && columnSpec <= lit(bounds(1).toDouble),
          columnSpec)
      } else if (!(x \ "aggregation" \ "default").text.isEmpty()) {
        val default = (x \ "aggregation" \ "default").text
        when(
          columnSpec.isNull,
          lit(default).cast(columnTypesMap.get((x \ "column").text).get))
          .otherwise(columnSpec)
      } else columnSpec

      val percentileArgs = (x \ "aggregation" \\ "argument")

      y ++ List(((x \ "aggregation" \ "type").text match {
        case "SUM"   => sum(column)
        case "MAX"   => max(column)
        case "MIN"   => min(column)
        case "COUNT" => count(column)
        case "APPROX_COUNT_DISTINCT" => {
          if (!(x \ "aggregation" \ "default").text.isEmpty()) {
            val default = (x \ "aggregation" \ "default").text.toDouble
            approx_count_distinct(column, default)
          } else approx_count_distinct(column, 0.03)

        }
        case "COUNT_DISTINCT" => {
          // countDistinct can take multiple columns and specifying them can be beneficial for performance while using partitioning
          val cols =
            (x \ "aggregation" \\ "argument").map { z => expr(z.text) } ++ List(column)
          countDistinct(cols.head, cols.tail: _*)
        }
        case "COUNT_DISTINCT_PARTITIONED" => approx_count_distinct(column)
        case "AVG"                        => avg(column)
        case "STDDEV"                     => stddev_pop(column)
        case "PERCENT"                    => avg(column * lit(100))
        case "HIVE_PERCENTILE" => {
          val percentiles = array(percentileArgs(0).text.split(",") map { z =>
            lit(z.toDouble / 100.0)
          }: _*)
          val maxSampleSize =
            if (percentileArgs.length >= 2) lit(percentileArgs(1).text.toInt)
            else lit(5000)
          val roundedColumnSpec =
            if (percentileArgs.length >= 3 && percentileArgs(
                2).text.toLowerCase == "true") round(column)
            else column
          callUDF(
            "hive_percentile_approx",
            roundedColumnSpec,
            percentiles,
            maxSampleSize
          ) // Returns a column of "Array" type
        }
        case "PERCENTILE_APPROX" => {
          val percentiles = array(percentileArgs(0).text.split(",") map { z =>
            lit(z.toDouble / 100.0)
          }: _*)
          val maxSampleSize =
            if (percentileArgs.length >= 2) lit(percentileArgs(1).text.toInt)
            else lit(5000)
          val roundedColumnSpec =
            if (percentileArgs.length >= 3 && percentileArgs(
                2).text.toLowerCase == "true") round(column)
            else column
          callUDF(
            "percentile_approx",
            roundedColumnSpec,
            percentiles,
            maxSampleSize
          ) // Returns a column of "Array" type
        }
        case "ALT_PERCENTILE_APPROX" => {
          val percentiles = percentileArgs(0).text.split(",") map { z => z.toInt }
          val maxSampleSize =
            if (percentileArgs.length >= 2) percentileArgs(1).text.toInt else 5000
          if (percentileArgs.length == 3) {
            if (percentileArgs(2).text.toLowerCase == "true")
              (new BenHaimTomApproximatePercentileV2(percentiles, maxSampleSize, true))(
                column)
            else if (percentileArgs(2).text.toLowerCase == "false")
              (new BenHaimTomApproximatePercentileV2(percentiles, maxSampleSize, false))(
                column)
            else
              (new BenHaimTomApproximatePercentileV2(
                percentiles,
                maxSampleSize,
                percentileArgs(2).text.toDouble))(column) // Error Spec
          } else {
            (new BenHaimTomApproximatePercentileV2(percentiles, maxSampleSize))(column)
          }
        }
        case "PERCENTILE_OF_PERCENTAGES" =>
          avg(column) * lit(100) // At this level, simply return percentages
        case "NUM_MISSING"     => sum(when(column.isNull, 1))
        case "NUM_NON_MISSING" => count(column)
        case "COLLECT_SET"     => collect_set(column)
        case _                 => lit(0)
      }).alias((x \ "name").text)) // Alias it with the fact name at the end

    })

    logger.info(s"FACT_SPECS: ${factSpecs.mkString("\n", "\n", "")}")
    groupedData.agg(factSpecs.head, factSpecs.tail: _*)

  }

  /**
   * Builds the cube from the input dataFrame
   * Returns a sequence of (rollup_name, rollup_result) tuples
   */
  def build(inputDataFrame: DataFrame): DataFrame = {

    // Fill empty string dimensions with literal __NULL__
    val dataFrame = (dimensions ++
      (hierarchies.flatten.toSet.toList) ++ (groupingSets.flatten.toSet.toList))
      .foldLeft(inputDataFrame)({ (frame, dim) =>
        frame.withColumn(dim, coalesce(col(dim).cast(StringType), lit("__NULL__")))
      })

    if (limitedTestMode)
      processCube(dataFrame.limit(10000))
    else
      processCube(dataFrame)

  }

  def toJsonString() = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val propsMap = Map(
      "baseDimensions" -> baseDimensions,
      "groupingSets" -> groupingSets,
      "mandatoryDimensions" -> mandatoryDimensions,
      "hierarchies" -> hierarchies,
      "xorBlacklists" -> xorBlacklists,
      "limitedDepthDimensions" -> limitedDepthDimensions,
      "ignoreNullGroupings" -> ignoreNullGroupings,
      "limitedTestMode" -> limitedTestMode,
      "hasOptionalTimeCube" -> optionalTimeCube.isDefined)
    write(propsMap)
  }
}
