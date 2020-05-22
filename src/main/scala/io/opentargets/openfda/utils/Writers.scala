package io.opentargets.openfda.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

object Writers extends LazyLogging {

  def writeMonteCarloResults(results: DataFrame, outputPath: String, extension: String): Unit = {

    extension match {
      case "csv" =>
        // write to one single compressed file
        logger.info("Writing monte carlo results as csv compressed output...")
        results
          .coalesce(1)
          .write
          .option("compression", "gzip")
          .option("header", "true")
          .csv(s"$outputPath/agg_critval_drug_csv/")

      case "json" =>
        logger.info("Writing monte carlo results as json output...")
        results.write
          .json(s"$outputPath/agg_critval_drug/")

      case err: String => logger.error(s"Unrecognised output format $err")
    }

  }

  def writeFdaResults(results: DataFrame, outputPath: String): Unit = {
    logger.info("Writing results of fda data transformation aggregated by Chembl...")
    results.write.json(s"$outputPath/agg_by_chembl/")
  }
}
