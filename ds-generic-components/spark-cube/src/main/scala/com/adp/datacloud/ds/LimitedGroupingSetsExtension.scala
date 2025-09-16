package com.adp.datacloud.ds

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

class LimitedGroupingSetsExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(_ => LimitedGroupingSetsAnalyzeRuleNew)
  }
}

object LimitedGroupingSetsAnalyzeRuleNew extends Rule[LogicalPlan] {

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def getBaseDimensions(): List[String] = {
    read[List[String]](System.getProperty("cube.base.dimensions"))
  }
  def getMinDepth(): Int = System.getProperty("cube.min.cube.depth").toInt

  def getMaxDepth(): Int = System.getProperty("cube.max.cube.depth").toInt

  def getFixedGroupingSets(): List[Set[String]] =
    read[List[Set[String]]](System.getProperty("cube.grouping.sets"))

  def getMandatoryDimensions(): List[String] =
    read[List[String]](System.getProperty("cube.mandatory.dimensions"))

  def getListOfHierarchies(): List[List[String]] =
    read[List[List[String]]](System.getProperty("cube.hierarchies"))

  def getXorBlacklists(): List[(String, Seq[String])] =
    read[List[(String, Seq[String])]](System.getProperty("cube.xor.blacklists"))

  def getLimitedDepthDimensions(): List[(String, Int)] =
    read[List[(String, Int)]](System.getProperty("cube.limited.depth.dimensions"))

  private val logger = Logger.getLogger(getClass)

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

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

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      /*
        org.apache.spark.sql.catalyst.plans.logical.Aggregate from spark-catalyst_*.jar has
        different signatures in databricks and open source. databricks has additional field - limit:Option[Int]
       */
      case p: Aggregate => {
        val groupingExpressions  = p.groupingExpressions
        val aggregateExpressions = p.aggregateExpressions
        if (plan.resolved && !plan.children.isEmpty && plan.children.size == 1) {
          logger.info(s"applying LimitedGroupingSetsAnalyzeRule...")

          val expandPlan = plan.children.head
          val optimizedPlan = expandPlan match {
            case Expand(projections, output, child) => {

              val baseDimensions: List[String]                = getBaseDimensions
              val minDepth: Int                               = getMinDepth
              val maxDepth: Int                               = getMaxDepth
              val fixedGroupingSets: List[Set[String]]        = getFixedGroupingSets
              val mandatoryDimensions: List[String]           = getMandatoryDimensions
              val listOfHierarchies: List[List[String]]       = getListOfHierarchies
              val xorBlacklists: List[(String, Seq[String])]  = getXorBlacklists
              val limitedDepthDimensions: List[(String, Int)] = getLimitedDepthDimensions

              logger.info(s"baseDimensions:$baseDimensions")
              logger.info(s"minCubeDepth:$minDepth")
              logger.info(s"maxCubeDepth:$maxDepth")
              logger.info(s"fixedGroupingSets:$fixedGroupingSets")
              logger.info(s"mandatoryDimensions:$mandatoryDimensions")
              logger.info(s"hierarchies:$listOfHierarchies")
              logger.info(s"xorBlacklists:$xorBlacklists")
              logger.info(s"limitedDepthDimensions:$limitedDepthDimensions")

              val numGroupingProjections = groupingExpressions.size

              // Filter groups
              val filteredGroupExpressions = projections filter { x =>
                // Limit the cube depth
                val currentGrouping =
                  x.takeRight(numGroupingProjections).reverse.tail.reverse
                val dimensionsSubsettedGrouping = (currentGrouping
                  .map { x => x.simpleString(1).split("#").head })
                  .toSet
                  .filter({ baseDimensions.contains(_) })
                dimensionsSubsettedGrouping.size >= minDepth && dimensionsSubsettedGrouping.size <= maxDepth
              } filter { x =>
                // Ensure that for all hierarchies, a valid combination is present in the grouping expression
                // Compute the list of valid combinations involving hierarchy columns (based on rollups)
                if (listOfHierarchies.isEmpty) true
                else {
                  val listOfHierarchyColumns = listOfHierarchies.flatten.toSet
                  val listOfRollups          = listOfHierarchies.map { y => rollups(y) }
                  val listOfValidGroupings = listOfRollups.tail
                    .foldLeft(listOfRollups.head)({ (acc, b) =>
                      (acc
                        .cross(b))
                        .toList
                        .map({ case (x, y) => (x ++ y).toSet.toList })
                    })
                    .map(_.sorted)
                    .toSet
                    .toList

                  val currentGrouping =
                    x.takeRight(numGroupingProjections).reverse.tail.reverse
                  val hierarchyColumnsSubsettedGrouping = (currentGrouping
                    .map { x => x.simpleString(1).split("#").head })
                    .toSet
                    .filter({ listOfHierarchyColumns.contains(_) })
                  listOfValidGroupings.contains(
                    hierarchyColumnsSubsettedGrouping.toList.sorted)
                }

              } filter { x =>
                // Remove blacklists
                val currentGrouping =
                  x.takeRight(numGroupingProjections).reverse.tail.reverse
                xorBlacklists.map(_._2).forall { y =>
                  val blackListSubsettedGrouping = (currentGrouping
                    .map { x => x.simpleString(1).split("#").head })
                    .toSet
                    .filter({ y.contains(_) })
                  blackListSubsettedGrouping.size <= 1
                }
              } filter { x =>
                // Keep only the groupings where mandatory dimensions are present
                val currentGrouping = x
                  .takeRight(numGroupingProjections)
                  .reverse
                  .tail
                  .reverse
                  .map({ x => x.simpleString(1).split("#").head })
                  .toSet
                mandatoryDimensions.forall { x => currentGrouping.contains(x) }
              } filter { x =>
                fixedGroupingSets.isEmpty || ({
                  // Filter explicitly specified grouping sets
                  val currentGrouping =
                    x.takeRight(numGroupingProjections).reverse.tail.reverse
                  val dimensionsSubsettedGrouping = (currentGrouping
                    .map { x => x.simpleString(1).split("#").head })
                    .toSet
                    .filter({ baseDimensions.contains(_) })
                  fixedGroupingSets.contains(dimensionsSubsettedGrouping)
                })
              } filter { x =>
                // Keep only the groupings where the limitedDepthDimensions are valid
                val currentGrouping = x
                  .takeRight(numGroupingProjections)
                  .reverse
                  .tail
                  .reverse
                  .map({ x => x.simpleString(1).split("#").head })
                  .toSet
                  .filter(x => x != null && x != "null")
                limitedDepthDimensions.foldLeft(true)((bool, y) => {
                  bool && !(currentGrouping.contains(y._1) && currentGrouping.size > y._2)
                })
              }

              logger.info("Computing the following Grouping Sets")
              val expressionDepths =
                filteredGroupExpressions
                  .map { x =>
                    {
                      val currentGrouping = x.takeRight(numGroupingProjections)
                      logger.info(currentGrouping.map { y =>
                        y.simpleString(1).split("#").head
                      } mkString ",")
                      val dimensionsSubsettedGrouping = (currentGrouping
                        .map { x => x.simpleString(1).split("#").head })
                        .toSet
                        .filter({ baseDimensions.contains(_) })
                      dimensionsSubsettedGrouping.size
                    }
                  }
                  .groupBy(identity)
                  .mapValues(_.size)
              logger.info(
                "Grouping Set Counts for each Depth level = " + expressionDepths)

              Expand(filteredGroupExpressions, output, child)
            }
            case _ => expandPlan
          }
          p.copy(child = optimizedPlan)
        } else plan
      }
      case _ => plan
    }

  }

}
