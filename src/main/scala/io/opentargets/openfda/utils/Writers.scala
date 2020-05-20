package io.opentargets.openfda.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

object Writers extends LazyLogging {

  def writeMonteCarloResults(results: DataFrame, outputPath: String): Unit = {
    logger.info("Writing results of monte carlo sampling...")
    results.write
      .json(s"$outputPath/agg_critval_drug/")

    // write to one single compressed file
    results
      .coalesce(1)
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"$outputPath/agg_critval_drug_csv/")
  }

  def writeFdaResults(results: DataFrame, outputPath: String): Unit = {
    logger.info("Writing results of fda data transformation...")
    results.write.json(s"$outputPath/agg_by_chembl/")
  }
}
