package com.adp.datacloud.ds

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.xml.XML

/**
 * Class for core business logic of generating insights
 *
 * @param Insights Configuration file path
 * @author Manoj Oleti
 *
 */
class InsightEngine(xmlConfigFileName: String,
    partitionColumns: Option[List[String]] = None,
    sparkShufflePartitions: Int = 200,
    minimumPercentageDifference:Double = 10)(implicit sparkSession: SparkSession) {
  
 private val RIGHT_SUFFIX = "_r"
  private val logger = Logger.getLogger(getClass())

  val rootElement = XML.load(xmlConfigFileName)
  val dimensions = ((rootElement \\ "dimensions" \\ "dimension").map { x => x.text }).toList
  val facts = ((rootElement \\ "facts" \\ "fact" \\ "name").map { x => x.text }).toList
  val factTypesMap = ((rootElement \\ "facts" \\ "fact" \\ "name").map { x => (x.text, 
      if (x.attribute("type").isDefined && x.attribute("type").get(0).text == "non-rate") 
        "non-rate"
      else 
        "rate") }).toMap
  val analysisGroups = ((rootElement \\ "analysisgroups" \\ "analysisgroup" \\ "name").map { x => x.text }).toList
  
  val insightsGroupName = (rootElement \ "name").text 
  val insightsType = (rootElement \ "type").text
  val insightsAccess = (rootElement \ "access").text
  val zscore_filt = (rootElement \\ "zscore_filter").text match {case "" => None
  case x => Some(x)
  }  
  
  val supportColumns = ((rootElement \\ "supportcolumns" \\ "column").map { x => x.text }).toList
  
  val insightScoreFunction = (rootElement \ "score_expression").text match { case "" => None 
    case expression => Some( (fact: String) => expr(expression.replaceAll("\\[fact\\]", fact)) ) 
  }
  
  val insightReasonFunction = (rootElement \ "reason_expression").text match { case "" => None 
    case expression => Some( (fact: String) => expr(expression.replaceAll("\\[fact\\]", fact)) ) 
  }
  
  val hierarchies = (rootElement \ "dimensions" \ "hierarchy").map { x => (x \ "dimension").map { _.text } toList } toList
  
  /**
   * Utility function to persist and checkpoint a dataframe
   */
  private def distributeUniformly = (dataFrame: DataFrame) => {
    partitionColumns match {
      case Some(x) => dataFrame.repartition(sparkShufflePartitions, x map { col(_) } :_*)
      case None => dataFrame.repartition(sparkShufflePartitions)
    }    
  }
  
  /**
   * Utility function to apply zsocre
   */
  private def zscore_filter = (dataFrame: DataFrame) => {
    zscore_filt match {
      case Some(x) => dataFrame.filter(x)
      case None => dataFrame
    }    
  }
  
  private def checkpointDataFrame = (dataFrame: DataFrame) => {
    val persistedDF = dataFrame
    //persistedDF.rdd.checkpoint
    //val checkpointedDF = distributeUniformly(
    //    sparkSession.createDataFrame(persistedDF.rdd, persistedDF.schema))
    val checkpointedDF = distributeUniformly(persistedDF)
    logger.info("Record count after checkpoint = " + checkpointedDF.count) // Action to materialize
    checkpointedDF
  }
  
  def splitHierarchyAt(at: String) = {
      val hierarchy = hierarchies.filter { x => x.contains(at) } 
      if(hierarchy.isEmpty) 
        (List[String](),List[String]())
      else{
        val splits = hierarchy(0).splitAt(hierarchy(0).indexOf(at))
        (splits._1, splits._2.tail)
      }
  }
  
  /**
   * Returns a function which takes a dataframe and returns a Tuple3 with the elements -
   *   1) Dataframe with Z-scores computed for the specified column(s).
   *   2) List of standard deviations for the specified column(s).
   *   3) List of means for the specified column(s).
   * The score columns will carry a "zscore_" prefix.
   */
  private def zscoresFor(columns: String*) = (df: DataFrame) => {
    // Get means, standard deviations and percentiles of the fact columns as another frame (summaryStats).
    // This frame shall have only one row    
    val summaryStatsSpecs = columns flatMap { x => List(avg(x).alias("mean_" + x), stddev_pop(x).alias("stddev_" + x))}
    val summaryStats = df.groupBy(analysisGroups map { col(_) } :_*).agg(summaryStatsSpecs.head, summaryStatsSpecs.tail :_*)
    
    //Join dataframes using null safe join
    val joinDf = df.join(summaryStats, analysisGroups.foldRight(lit(true)) { (z,accumulatorSpec) => accumulatorSpec.and (df(z) <=> summaryStats(z))})
    
    //Get unique columns from the joined dataframe
    val allColumns = joinDf.columns.toSet
    
    //Select appropriate columns from the joined dataset as it contains joining columns from both dataframes.
    val selColDf = joinDf.select(allColumns.toSeq  map {x => if(df.columns.toSet.contains(x) || analysisGroups.contains(x)) df(x) else summaryStats(x)} :_*)
    
    //Compute zscore
    columns.foldLeft(selColDf) ( { (dataFrame, column) => 
      dataFrame.withColumn("zscore_" + column, 
          when(col("stddev_" + column).notEqual(lit(0)).and(col(column).notEqual(lit(0))), (col(column) - col("mean_" + column)) / col("stddev_" + column))).
          withColumn("mean_" + column,col("mean_" + column)).
          withColumn("stddev_"+ column,col("stddev_" + column))
    } )

  }
  
  private def logTransform(columns: String*) = (df: DataFrame) => {
    columns.foldLeft(df)({(dataFrame,column) =>
      dataFrame.withColumn("norm_"+ column, log(col(column)))
    })
  }
  
  /**
   *  Adds three columns for a given fact
   *  1) insight_reason (ABS_DIFF, PCTG_DIFF, PCTL_SCORE_DIFF) 
   *  2) insight_type defined in the insight XML file (One of CLIENT_INTERNAL, CLIENT_VS_BM, BUSINESS_OUTCOMES)
   *  3) insight_score (heuristic built with percentage_difference, percentage_headcount, quarterly vs monthly vs yearly, num_non_null_dimensions)
   */
  private def insightScoreFor(column: String) = (df: DataFrame) => {

    // TODO: Make this more elegant
    df.withColumn("insight_type_" + column, lit(insightsType)).
        withColumn("insight_reason_" + column, 
            insightReasonFunction match {
            case None => 
                  when(col("percent_rank_"+column).isNotNull, lit("PERCENTILE_RANKING")).                 
                  when(abs(col("zscore_diff_" + column)/lit(3)).equalTo(
                            greatest(abs(col("zscore_diff_" + column)/lit(3)), 
                              abs(col("zscore_pctg_diff_" + column)), 
                              abs(col("zscore_norm_diff_" + column)), 
                              abs(col("zscore_norm_pctg_diff_" + column)))), lit("ABS_DIFF")).
                   when(abs(col("zscore_pctg_diff_" + column)).equalTo(
                            greatest(abs(col("zscore_diff_" + column)/lit(3)), 
                              abs(col("zscore_pctg_diff_" + column)), 
                              abs(col("zscore_norm_diff_" + column)), 
                              abs(col("zscore_norm_pctg_diff_" + column)))), lit("PCTG_DIFF")).
                   when(abs(col("zscore_norm_diff_" + column)).equalTo(
                            greatest(abs(col("zscore_diff_" + column)/lit(3)), 
                              abs(col("zscore_pctg_diff_" + column)), 
                              abs(col("zscore_norm_diff_" + column)), 
                              abs(col("zscore_norm_pctg_diff_" + column)))), lit("NORM_ABS_DIFF")).
                   when(abs(col("zscore_norm_pctg_diff_" + column)).equalTo(
                            greatest(abs(col("zscore_diff_" + column)/lit(3)), 
                              abs(col("zscore_pctg_diff_" + column)), 
                              abs(col("zscore_norm_diff_" + column)), 
                              abs(col("zscore_norm_pctg_diff_" + column)))), lit("NORM_PCTG_DIFF")).
                    otherwise(lit("NO_INSIGHT"))                     
            case Some(functionExpression) => functionExpression(column)
          }).
        withColumn("insight_score_" + column, 
          round(insightScoreFunction match {
            case None => 
                  abs(col("zscore_diff_" + column) + when(col("zscore_pctg_diff_" + column).isNotNull, col("zscore_pctg_diff_" + column)).otherwise(lit(0))) * 
                  when(col("pctg_diff_" + column).isNotNull,abs(col("pctg_diff_" + column))).otherwise(lit(1))
            case Some(functionExpression) => functionExpression(column)
          },2) )
  }
  
  /**
   * A function which takes a dataframe generates diffs and percentage diffs for facts in it 
   */
  private val generateDiffs = (dataFrame: DataFrame) => {
    
    factTypesMap.foldLeft(dataFrame) { (df, factAndTypePair) =>
      val nonrate: String = "non-rate"
      factAndTypePair match {
        case (x, `nonrate`) => {
          val ephemeralDimensions = ((rootElement \\ "dimensions" \\ "dimension").filter {x => x.attribute("type").isDefined && x.attribute("type").get(0).text == "ephemeral" } map { x => x.text }).toList
          val ephemeralValidationSpec = not(ephemeralDimensions.foldLeft(lit(true))( { (colSpec, x) => colSpec.and(col(x) <=> col(x + RIGHT_SUFFIX)) } ))
          // The diff should be generated ONLY when an ephemeral dimension is different across left and right
          df.withColumn("diff_" + x, when(col(x).notEqual(lit(0)).and(col(x + RIGHT_SUFFIX).notEqual(lit(0))).and(ephemeralValidationSpec),col(x) - col(x + RIGHT_SUFFIX))). // Absolute difference
            withColumn("pctg_diff_" + x, when(col(x).notEqual(lit(0)).and(col(x + RIGHT_SUFFIX).notEqual(lit(0))).and(ephemeralValidationSpec),(col(x) - col(x + RIGHT_SUFFIX)) * 100 / col(x + RIGHT_SUFFIX))) // Percentage difference
        }
        case (x, _) => df.withColumn("diff_" + x, when(col(x).notEqual(lit(0)).and(col(x + RIGHT_SUFFIX).notEqual(lit(0))),col(x) - col(x + RIGHT_SUFFIX))). // Absolute difference
          withColumn("pctg_diff_" + x, when(col(x).notEqual(lit(0)).and(col(x + RIGHT_SUFFIX).notEqual(lit(0))),(col(x) - col(x + RIGHT_SUFFIX)) * 100 / col(x + RIGHT_SUFFIX))) // Percentage difference
      }
    }
  }
  
  /**
   * A function which takes a dataframe generates zscores for diffs found in it 
   */
  private val generateZscores = (dataFrame: DataFrame) => {
    val df = dataFrame // We persist to avoid duplicate computation while performing summary stats aggregations in zscoresFor method.
    val columnsNeedingZscores = facts flatMap {p => List("diff_" + p, "pctg_diff_" + p,"norm_diff_" + p,"norm_pctg_diff_" + p) }
    val result = zscoresFor(columnsNeedingZscores : _*) (df)
    result
  }
    
  /**
   * A function to distribute the diff and pctg_diff column to Normal Distribution.Replace all diff columns which have NaN with zero as this function uses log2 function 
   */
  private val normalizeDiffs =(dataFrame:DataFrame) => {
    val columnsNeedingNormalDist = facts flatMap {p => List("diff_" + p, "pctg_diff_" + p) }
    logTransform(columnsNeedingNormalDist : _*) (dataFrame)
  }
  
  /**
   * A function which takes a dataframe and adds insightScores for facts found in it 
   */
  private val generateInsightScores = (dataFrame: DataFrame) => {
    facts.foldLeft(dataFrame)((df,column) => {
      insightScoreFor(column)(df)
    })
  }
  
   /**
   * A function which takes a dataframe and adds insightScores for facts found in it 
   */
   private val generateInsightMetadata = (dataFrame: DataFrame) => {

    // Metrics of facts in an array column
    val factSpecs = facts.map { x => 
      array(round(col(x),2), 
        round(col(x + RIGHT_SUFFIX),2),
        round(col(x+"_events"),2),
        round(col(x+"_events" + RIGHT_SUFFIX),2),
        round(col("diff_"+x),2),
        round(col("pctg_diff_"+x),2),
        round(col("norm_diff_"+x),2),
        round(col("norm_pctg_diff_"+x),2),
        round(col("zscore_diff_"+x),2),
        round(col("zscore_pctg_diff_"+x),2),
        round(col("zscore_norm_diff_"+x),2),
        round(col("zscore_norm_pctg_diff_"+x),2),
        col("percent_rank_"+x),
        round(col("min_"+x),2),
        round(col("max_"+x),2),
        col("zscore_"+x),
        col("zscore_norm_"+x),
        round(col("mean_diff_"+x),2),
        round(col("stddev_diff_"+x),2),
        round(col("mean_pctg_diff_"+x),2),
        round(col("stddev_pctg_diff_"+x),2),
        round(col("mean_norm_diff_"+x),2),
        round(col("stddev_norm_diff_"+x),2),
        round(col("mean_norm_pctg_diff_"+x),2),
        round(col("stddev_norm_pctg_diff_"+x),2),
        round(col("mean_"+x),2),
        round(col("stddev_"+x),2),
        round(col("mean_norm_"+x),2),
        round(col("stddev_norm_"+x),2)).alias(x + "_metrics")
        }
    
    // Number of applicable dimensions in this column. Each hierarchy should be counted as a single dimension (hence the use of "head") 
    val baseDimensions = (hierarchies.map { x => x.head } ++ dimensions.filter { x => hierarchies.forall { y => !y.contains(x) } })
    val numDimensions = baseDimensions.foldLeft(lit(0))( { (accumulator, thisDim) => accumulator.plus(when(col(thisDim).isNotNull, lit(1)).otherwise(lit(0)))} ).alias("num_dimensions")
    
    dataFrame.select((dataFrame.columns map {col(_)}) ++ factSpecs :+ numDimensions :_*)
    
  }
  
  /**
   * Generates comparisons using cubes
   */
  private def buildComparisonsWith(right : DataFrame) = (left: DataFrame) => {
    // insightDimensions is a list of tuple4s (name, type, differences, alwayson) of 
    // each dimension which can potentially vary between left/right to generate a comparison
    val allDimensions = (rootElement \\ "dimension").map { x =>
      (x.text, x.attribute("type") match {
        case Some(y) => y.text
        case None => "invariant"
      }, x.attribute("differences") match {
        case Some(y) => y.text.split(",").map { z => z.toInt } toList
        case None => List[Int]()
      },x.attribute("alwayson") match {     //This should be for Null Variant dimensions Only
        case Some(y) => y.text.toBoolean
        case None => false
      })
    } 
    val insightDimensions = allDimensions filter { x => x._2 != "invariant" } filter(x=> !x._4)
    
    
    val joinDimensions = allDimensions filter { x => x._2 == "invariant" } map (x => x._1)
    val listOfJoinPredicates = joinDimensions.foldRight(
        lit(true)) { (z, colSpec) =>
          colSpec.and(col(z).eqNullSafe(col(z + RIGHT_SUFFIX)))}
    
    // Perform join for generating diffs 
    val df = right.join(left, listOfJoinPredicates)
   
    val listOfConjunctions = insightDimensions.map({ y =>
      allDimensions.foldRight(
        lit(true)) { (z, accumulatorSpec) =>
          accumulatorSpec.and(if (z._1 != y._1) {
            if (z._4) {
              // alwayson case.
              col(z._1).isNotNull.and(col(z._1 + RIGHT_SUFFIX).isNull)
            } else {
              if (y._2 != "ephemeral" | (y._2 == "ephemeral" && z._2 != "ephemeral")) {
                col(z._1).eqNullSafe(col(z._1 + RIGHT_SUFFIX))
              } else {
                lit(true)
              }
            }
          } else {
            val differingColumnSpec = y._2 match {
              case "variant" => col(z._1).isNotNull.and(col(z._1).gt(col(z._1 + RIGHT_SUFFIX))) // NotEqual Condition returns both x != y and y != x comparisons. Greater than Condition will return single set of Pair.
              case "nullvariant" => col(z._1).isNotNull.and(col(z._1 + RIGHT_SUFFIX).isNull)
              case "ephemeral" => {
                col(z._1).minus(col(z._1 + RIGHT_SUFFIX)).isin(y._3 :_*).and(col(z._1).isNotNull) // Ephemeral comparisons are against history (so no need for abs function)
              }
            }
            
            // If the current column is a part of a hierarchy apply surrogate predicates
            val hierarchyParts = splitHierarchyAt(z._1) 
            hierarchyParts._2.foldLeft(
                hierarchyParts._1.foldLeft(differingColumnSpec) ( { (x,y) => x.and(col(y).isNotNull) }))(
                    { (x,y) => x.and(col(y).isNull).and(col(y + RIGHT_SUFFIX).isNull)})
          })
        }
    })
    
    // Return a union of all comparison dataFrames
    val unionDf = listOfConjunctions match {
      case Nil => df
      case _ => listOfConjunctions.map { x => df.filter(x) }.reduce((df1, df2) => df1.unionAll(df2))
    }
    
    //Apply zscore filters if any before calculating zscores
    zscore_filter(unionDf)
  }
  
  /**
   * Generate ranks for defined percentage groups
   */
  private val generateRankBasedInsights = (left: DataFrame) => {
    /**
     * Filter the frame with non-null values for this current dimension and null values for all other dimensions
     */    
    def sliceBy(percentileDimensions: Seq[String],
        hierarchyPercentileDimensions: Seq[Seq[String]],
        partitions: Seq[String] = List()) = {
      
      // Remove the record where all dimensions are nulls
      val allNullFilterSpec = not(dimensions.tail.foldLeft(col(dimensions.head).isNull) { (x,y) => x.and(col(y).isNull)})
      
      // Remove records where non-percentile dimensions are populated.
      val percentileOnlyDimensionsSpec = dimensions.filter { y => !percentileDimensions.contains(y) && !hierarchyPercentileDimensions.flatten.contains(y) && !partitions.contains(y)}.
        foldLeft(allNullFilterSpec)( { (x,y) => x.and(col(y).isNull)} )
        
      // Keep records where ONLY one of percentile dimension is populated
      val singlePercentileDimensionSpec = (percentileDimensions ++ hierarchyPercentileDimensions.map {_.head}).foldLeft(lit(0))((a,b) => {
        a.plus(when(col(b).isNotNull, lit(1)).otherwise(lit(0)))
      }).equalTo(lit(1))
      
      allNullFilterSpec.and(percentileOnlyDimensionsSpec).and(singlePercentileDimensionSpec)
      
    }
    
    /**
     * Computes percent_rank of each record along a given dimension for each fact 
     */
    def generatePercentRanks (partitions: Seq[String],
        percentileDimensions: Seq[String]) = (dataFrame: DataFrame) => {          
      
      val partitionSpec = percentileDimensions.map({ x => col(x).isNull.alias(x + "_group") }) ++ partitions.map({ col(_) })
      val zscorePartitionSpec = percentileDimensions.map({ x => col(x).isNull.alias(x + "_group") }) ++ analysisGroups.map({ col(_) })
      
      val normDf = logTransform(facts : _*) (dataFrame)    
      val columnsNeedingZscores = facts flatMap {p => List("norm_" + p, p) }      
      
      val summaryStatsSpecs = columnsNeedingZscores flatMap { x => List(avg(x).alias("mean_" + x), stddev_pop(x).alias("stddev_" + x))}
      val summaryStats = normDf.groupBy(zscorePartitionSpec map { x => x } :_*).agg(summaryStatsSpecs.head, summaryStatsSpecs.tail :_*)
      
      
      //Adding required columns inorder to do join with grouped data
      val df_with_join_cols = percentileDimensions.foldRight(normDf)({ (p_col, df) =>
        df.withColumn(p_col + "_group",when(col(p_col).isNull,true).otherwise(false))
      })
      
      val df_with_perc_ranks = facts.foldRight(df_with_join_cols)({ (fact, df) =>
        val rankPartitionSpec = if (partitions.isEmpty) Window.orderBy(fact) else Window.partitionBy(partitionSpec :_*)
        df.withColumn("percent_rank_" + fact, lit(100)*percent_rank.over(rankPartitionSpec.orderBy(fact))).
           withColumn("min_" + fact, min(fact).over(rankPartitionSpec)).
           withColumn("max_" + fact, max(fact).over(rankPartitionSpec))
          })
      
      val joinCols = analysisGroups ++ percentileDimensions.map(x => x + "_group")
      
      //Join both dataframes using null safe joins
      val joinDf = df_with_perc_ranks.join(summaryStats, joinCols.foldRight(lit(true)) { (z,accumulatorSpec) => accumulatorSpec.and (df_with_perc_ranks(z) <=> summaryStats(z))})
      
      //Get the unique columns from the joined dataframe
      val allColumns = joinDf.columns.toSet
      
      //Select appropriate columns from the joined frame as we contain joining columns from both dataframes
      val selColDf = joinDf.select(allColumns.toSeq  map {x => if(df_with_perc_ranks.columns.toSet.contains(x) || joinCols.contains(x)) df_with_perc_ranks(x) else summaryStats(x)} :_*)
      
      // Compute Z-scores for all fact columns
      columnsNeedingZscores.foldLeft(selColDf) ( { (dataFrame, column) => 
      dataFrame.withColumn("zscore_" + column, 
          when(col("stddev_" + column).notEqual(lit(0)).and(col(column).notEqual(lit(0))), (col(column) - col("mean_" + column)) / col("stddev_" + column))).
           withColumn("mean_" + column,col("mean_" + column)).
           withColumn("stddev_"+ column,col("stddev_" + column))
    } )
    }
    
    val percentileDimensions = (rootElement \ "dimensions" \ "dimension").
        filter { y => y.attribute("percentile_rank").isDefined  && y.attribute("percentile_rank").get(0).text == "true" } map {_.text} 
        
    val hierarchyPercentileDimensions = (rootElement \ "dimensions" \ "hierarchy").
        filter { y => y.attribute("percentile_rank").isDefined  && y.attribute("percentile_rank").get(0).text == "true" } map ({ x => (x \ "dimension") map { _.text } })
    
    if (percentileDimensions.size + hierarchyPercentileDimensions.size == 0) {
      logger.info("No ranks to generate ")
      facts.foldLeft(left)({ 
        (df, fact) => df.withColumn("percent_rank_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("min_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("max_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("zscore_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("zscore_norm_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("mean_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("stddev_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("mean_norm_" + fact, lit(null.asInstanceOf[Double])).
                         withColumn("stddev_norm_" + fact, lit(null.asInstanceOf[Double]))
        })
    } else {
      logger.info("Generating ranks for " + percentileDimensions.mkString(","))
      // If ranks are to be computed within certain partitions (typically done only w.r.t time - so implemented only for hierarchies right now)
      val partitions = ((rootElement \\ "hierarchy")
                          filter { z => z.attribute("percentile_rank_partition").isDefined && z.attribute("percentile_rank_partition").get(0).text == "true"}
                          flatMap ({ x => (x \ "dimension") map { _.text } }) toList
                       ) ++
                       ((rootElement \\ "dimension")
                           filter { z => z.attribute("percentile_rank_partition").isDefined && z.attribute("percentile_rank_partition").get(0).text == "true"}
                           map ({_.text}) toList)
      
      val filterSpecForFacts = sliceBy(percentileDimensions, hierarchyPercentileDimensions, partitions ++ analysisGroups)
      (generatePercentRanks(partitions ++ analysisGroups, percentileDimensions ++ hierarchyPercentileDimensions.flatten)) (left.filter(filterSpecForFacts))
    }
    
  }
  
  /**
   * Differencing based Method to generate insights given two cubes for comparison
   */
  private def generateInsights(thisCube: DataFrame) = (thatCube: DataFrame) => {
    
    val rankBasedInsightsDf = checkpointDataFrame(generateRankBasedInsights(zscore_filter(thisCube)))
    
    val diffBasedInsightsDf = checkpointDataFrame((buildComparisonsWith(thatCube)
        andThen generateDiffs
        andThen normalizeDiffs
        andThen generateZscores
    ) (thisCube))
    
    val commonColumns = rankBasedInsightsDf.columns.toSet.intersect(diffBasedInsightsDf.columns.toSet)
    val addonRankColumns = rankBasedInsightsDf.schema.fields.filter({ x => !commonColumns.contains(x.name) })
    val addonDiffColumns = diffBasedInsightsDf.schema.fields.filter({ x => !commonColumns.contains(x.name) })
    
    /**
     * Decomposed function to add a generic column to a dataFrame
     */
    def addColumn = (df: DataFrame, column: StructField) =>
      df.withColumn(column.name, column.dataType match {
        case IntegerType => lit(null.asInstanceOf[Integer])
        case DoubleType => lit(null.asInstanceOf[Double])
        case LongType => lit(null.asInstanceOf[Long])
        case FloatType => lit(null.asInstanceOf[Float])
        case StringType => lit(null.asInstanceOf[String])
        case _ => lit(null.asInstanceOf[String])
      })
    
    val modifiedRankInsightsDf = addonDiffColumns.foldLeft(rankBasedInsightsDf)( {(df, column) => addColumn(df, column)} )
    val modifiedDiffInsightsDf = addonRankColumns.foldLeft(diffBasedInsightsDf)( {(df, column) => addColumn(df, column)} ) 
    
    val unionedDf = modifiedDiffInsightsDf.unionAll(modifiedRankInsightsDf.select(modifiedDiffInsightsDf.columns map {col(_)} :_*))
    generateInsightMetadata(unionedDf)
    
  }
  
  /**
   * Method to generate insights given two cubes for comparison
   */
  def build(thisCube: DataFrame)(thatCube: DataFrame = thisCube) = {
    val insightResult = (generateInsights(thisCube) andThen generateInsightScores) (thatCube)
    val insightSpecs = facts.flatMap { x => 
      List(col("insight_type_" + x),
          col("insight_reason_" + x),
          col("insight_score_" + x),
          col(x + "_metrics")) }
    
    val dimensionColumnSpecs = dimensions.flatMap { x => List(x, x + RIGHT_SUFFIX) } map {col(_)} 
    val supportColumnSpecs = supportColumns flatMap { x => List(col(x), col(x + RIGHT_SUFFIX)) }
    val insightFilterSpec = not(facts.foldRight(lit(true)) { (x, accumulatorSpec) => accumulatorSpec.and(col("insight_reason_" + x).equalTo("NO_INSIGHT")) })
    insightResult.filter(insightFilterSpec).select(dimensionColumnSpecs ++ insightSpecs ++ supportColumnSpecs :+ col("num_dimensions") :_*)
  }
  
}