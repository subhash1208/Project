package com.adp.datacloud.ml

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

trait WorkflowStep {

  def run(df: DataFrame): DataFrame

  def needsCheckpoint: Boolean = false

}

abstract class AbstractWorkflowStep extends WorkflowStep {

  def checkpoint(dataFrame: DataFrame, partitionExprs: Column*)(implicit
      sparkSession: SparkSession) = {
    dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
    dataFrame.rdd.checkpoint()
    dataFrame.take(1) // Force checkpoint
    sparkSession
      .createDataFrame(dataFrame.rdd, dataFrame.schema)
      .repartition(partitionExprs: _*)
  }

}

class WorkflowBuilder(
    private val spark: SparkSession,
    private val dataFrame: DataFrame,
    private val partitionColumns: Option[List[String]] = None) {

  private val logger = Logger.getLogger(getClass())

  def time[A](prefix: String)(a: => A) = {
    val t0      = System.currentTimeMillis()
    val result  = a
    val elapsed = (System.currentTimeMillis() - t0) / 1000
    println(prefix + " " + elapsed + " seconds")
    result
  }

  /**
   * Applies the Workflow step on the dataframe and returns it
   */
  def apply(workflowStep: WorkflowStep) = {
    logger.info("Running " + workflowStep.getClass)

    // Run the step and create a local checkpoint
    // Checkpoint is critical because the lineage of RDDs can get very long
    time("Running " + workflowStep.getClass + " finished in ")({
      val result = workflowStep.run(dataFrame)
      val newDf = if (workflowStep.needsCheckpoint) {
        result.persist(StorageLevel.MEMORY_AND_DISK_SER)
        result.rdd.checkpoint() // Checkpoint the RDD & truncate the lineage
        result.rdd.take(
          1
        ) // Call an action to execute the checkpoint. Ignore results of the action
        // Create dataFrame from checkpointed RDD
        val newFrame = partitionColumns match {
          case Some(x) =>
            spark
              .createDataFrame(result.rdd, result.schema)
              .repartition(x map { col(_) }: _*)
          case None => spark.createDataFrame(result.rdd, result.schema)
        }
        result.unpersist()
        newFrame
      } else result
      new WorkflowBuilder(spark, newDf)
    })

  }

  /**
   * Utility method to apply multiple workflow steps in a row.
   */
  def apply(workflowSteps: List[WorkflowStep]) =
    new WorkflowBuilder(
      spark,
      workflowSteps.foldRight(dataFrame)((workflowStep, df) => workflowStep.run(df)))

  def toDF() = dataFrame

}
