package io.opentargets.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.openfda.config.ETLSessionContext
import org.apache.spark.sql.DataFrame

object StratifiedSampling extends LazyLogging {

  /*
  - rawFda is the data that comes directly from the FDA without any changes
  - cleanFda is the data after it has been cleaned: filtered by blacklist, qualifications of reporter, patient
  death, etc.
  - significantFda is the data that has been prepared for MC sampling, as at this point we have already removed all
  log-likelihood rations that are effectively zero.
   */
  def apply(rawFda: DataFrame,
            cleanFda: DataFrame,
            significantFda: DataFrame,
            sampleSize: Double = 0.1)(implicit context: ETLSessionContext): Unit = {

    val idCol = "chembl_id"
    logger.debug("Generating ChEMBL ids for sample")
    val significantChembls = significantFda.select(idCol).distinct.sample(sampleSize)
    val allChembls = cleanFda.select(idCol).distinct.sample(sampleSize)

    val sampleOfChemblIds: DataFrame =
      significantChembls.join(allChembls, Seq(idCol), "full_outer").distinct

    // this leaves us with the report ids from the original data
    logger.debug("Converting ChEMBL ids to safetyreportid")
    val reportIds = cleanFda
      .select(idCol, "safetyreportid")
      .join(sampleOfChemblIds, Seq(idCol))
      .drop(idCol)
      .distinct()

    logger.info("Writing statified...")
    rawFda
      .join(reportIds, Seq("safetyreportid"))
      .write
      .json(context.configuration.fda.sampling.output)

  }

}
