package io.opentargets.openfda

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.openfda.config.ETLSessionContext
import io.opentargets.openfda.stage.{MonteCarloSampling, OpenFdaEtl}
import io.opentargets.openfda.utils.Writers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object ETL extends LazyLogging {

  def applySingleStep(step: String)(implicit context: ETLSessionContext): Unit = {
    implicit val sc: SparkSession = context.sparkSession
    step match {
      case "fda" =>
        logger.info("run step fda pipeline...")
        val fdaConfig = context.configuration.fda
        logger.info("Aggregating FDA data...")
        val openFdaDataAggByChembl: DataFrame =
          OpenFdaEtl(context).persist(StorageLevel.MEMORY_AND_DISK_SER)

        logger.info("Performing Monte Carlo sampling...")
        val mcResults =
          MonteCarloSampling(
            openFdaDataAggByChembl,
            fdaConfig.montecarlo.percentile,
            fdaConfig.montecarlo.permutations).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // write results if necessary
        logger.info("Writing results of FDA pipeline...")

        Writers.writeFdaResults(openFdaDataAggByChembl, context.configuration.common.output)

        if (fdaConfig.outputs.nonEmpty) {
          fdaConfig.outputs.foreach { extension =>
            Writers.writeMonteCarloResults(mcResults,
                                           context.configuration.common.output,
                                           extension)
          }

        }

        logger.info("All FDA stages complete.")
    }
  }

  def apply(steps: Seq[String]): Unit = {

    implicit val etlContext: ETLSessionContext = ETLSessionContext()

    logger.debug(etlContext.configuration.toString)

    val etlSteps =
      if (steps.isEmpty) etlContext.configuration.common.defaultSteps
      else steps

    val unknownSteps = etlSteps.toSet diff etlContext.configuration.common.defaultSteps.toSet
    val knownSteps = etlSteps.toSet intersect etlContext.configuration.common.defaultSteps.toSet

    logger.info(s"valid steps to execute: ${knownSteps.toString}")
    logger.warn(s"invalid steps to skip: ${unknownSteps.toString}")

    knownSteps.foreach { step =>
      logger.debug(s"step to run: '$step'")
      ETL.applySingleStep(step)
    }

  }

}
