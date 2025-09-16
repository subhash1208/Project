package com.adp.datacloud.ds

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Resolution rule hack to limit the number of grouping sets based on depth or hierarchies or fixed grouping sets
 */
@Deprecated
@deprecated("Use LimitedGroupingSetsAnalyzeRule instead", "5.0")
case class LimitedGroupingSetsOptimizeRule(
  baseDimensions:         List[String]                = List(),
  minDepth:               Int                         = 0,
  maxDepth:               Option[Int]                 = None,
  fixedGroupingSets:      List[Set[String]]           = List(),
  mandatoryDimensions:    List[String]                = List(),
  listOfHierarchies:      List[List[String]]          = List(),
  xorBlacklists:          List[(String, Seq[String])] = List(),
  limitedDepthDimensions: List[(String, Int)]         = List()) extends Rule[LogicalPlan] {

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

  /**
   * calculate the count of default combinations that cube generates with available dimensions
   * @return
   */
  lazy val maxProjectionsCount = {
    val numDimensions = (listOfHierarchies.flatMap(x => x) ::: baseDimensions ::: mandatoryDimensions).distinct.size
    scala.math.pow(2, numDimensions).toInt
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case Expand(projections, output, child) => {
        if (projections.length < maxProjectionsCount) {
          logger.info("grouping already seems optimized must be next iteration of rule being applied, hence skipping optimization")
          Expand(projections, output, child)
        } else {
          val groupExprsMask = projections.map({ _.last.toString.toInt }).max * 2 + 1
          logger.debug("groupExprsMask = " + groupExprsMask + "(" + groupExprsMask.toBinaryString + ")")

          // Filter groups
          val filteredGroupExpressions = projections filter { x =>
            // Limit the cube depth
            val currentGrouping = x.takeRight(Integer.bitCount(groupExprsMask)).reverse.tail.reverse
            val dimensionsSubsettedGrouping = (currentGrouping.map { x => x.simpleString(1).split("#").head }).toSet.filter({ baseDimensions.contains(_) })
            dimensionsSubsettedGrouping.size >= minDepth && (maxDepth match {
              case Some(x) => dimensionsSubsettedGrouping.size <= x
              case None    => true
            })
          } filter { x =>
            // Ensure that for all hierarchies, a valid combination is present in the grouping expression
            // Compute the list of valid combinations involving hierarchy columns (based on rollups)
            if (listOfHierarchies.isEmpty) true
            else {
              val listOfHierarchyColumns = listOfHierarchies.flatten.toSet
              val listOfRollups = listOfHierarchies.map { y => rollups(y) }
              val listOfValidGroupings = listOfRollups.tail.foldLeft(listOfRollups.head)({
                (acc, b) => (acc.cross(b)).toList.map({ case (x, y) => (x ++ y).toSet.toList })
              }).map(_.sorted).toSet.toList

              val currentGrouping = x.takeRight(Integer.bitCount(groupExprsMask)).reverse.tail.reverse
              val hierarchyColumnsSubsettedGrouping = (currentGrouping.map { x => x.simpleString(1).split("#").head }).toSet.filter({ listOfHierarchyColumns.contains(_) })
              listOfValidGroupings.contains(hierarchyColumnsSubsettedGrouping.toList.sorted)
            }

          } filter { x =>
            // Remove blacklists
            val currentGrouping = x.takeRight(Integer.bitCount(groupExprsMask)).reverse.tail.reverse
            xorBlacklists.map(_._2).forall { y =>
              val blackListSubsettedGrouping = (currentGrouping.map { x => x.simpleString(1).split("#").head }).toSet.filter({ y.contains(_) })
              blackListSubsettedGrouping.size <= 1
            }
          } filter { x =>
            // Keep only the groupings where mandatory dimensions are present
            val currentGrouping = x.takeRight(Integer.bitCount(groupExprsMask)).reverse.tail.reverse.map({ x => x.simpleString(1).split("#").head }).toSet
            mandatoryDimensions.forall { x => currentGrouping.contains(x) }
          } filter { x =>
            fixedGroupingSets.isEmpty || ({
              // Filter explicitly specified grouping sets
              val currentGrouping = x.takeRight(Integer.bitCount(groupExprsMask)).reverse.tail.reverse
              val dimensionsSubsettedGrouping = (currentGrouping.map { x => x.simpleString(1).split("#").head }).toSet.filter({ baseDimensions.contains(_) })
              fixedGroupingSets.contains(dimensionsSubsettedGrouping)
            })
          } filter { x =>
            // Keep only the groupings where the limitedDepthDimensions are valid
            val currentGrouping = x.takeRight(Integer.bitCount(groupExprsMask)).reverse.tail.reverse.map({ x => x.simpleString(1).split("#").head }).toSet.filter(x => x != null && x != "null")
            limitedDepthDimensions.foldLeft(true)((bool, y) => {
              bool && !(currentGrouping.contains(y._1) && currentGrouping.size > y._2)
            })
          }

          logger.info("Computing the following Grouping Sets")
          val expressionDepths = filteredGroupExpressions.map { x =>
            {
              val currentGrouping = x.takeRight(Integer.bitCount(groupExprsMask))
              logger.info(currentGrouping.map { y => y.simpleString(1).split("#").head } mkString ",")
              val dimensionsSubsettedGrouping = (currentGrouping.map { x => x.simpleString(1).split("#").head }).toSet.filter({ baseDimensions.contains(_) })
              dimensionsSubsettedGrouping.size
            }
          }.groupBy(identity).mapValues(_.size)
          logger.info("Grouping Set Counts for each Depth level = " + expressionDepths)
          if (filteredGroupExpressions.length == projections.length) {
            // returning the same tree when there is no gain in optimization saves us from rerunning the rule by rule executor
            Expand(projections, output, child)
          } else {
            Expand(filteredGroupExpressions, output, child)
          }
        }
      }
    }

}

