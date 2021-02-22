package io.opentargets.openfda.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object Writers extends LazyLogging {

  def writeMonteCarloResults(results: DataFrame, outputPath: String, extension: String)(
      implicit sparkSession: SparkSession): Unit = {

    extension match {
      case "csv" =>
        // write to one single compressed file
        logger.info("Writing monte carlo results as csv compressed output...")
        val fileName = s"$outputPath/agg_critval_drug_csv/"
        results
          .coalesce(1)
          .write
          .option("compression", "gzip")
          .option("header", "true")
          .csv(s"$outputPath")

      case "json" =>
        logger.info("Writing monte carlo results as json output...")
        results.write
          .json(s"$outputPath")
      case "parquet" =>
        logger.info("Writing monte carlo results as parquet output...")
        results.write
          .format("parquet")
          .save(s"$outputPath")

      case err: String => logger.error(s"Unrecognised output format $err")
    }

  }

  def writeFdaResults(results: DataFrame, outputPath: String, extension: String): Unit = {
    logger.info("Writing results of fda data transformation aggregated by Chembl.")
    extension match {
      case "json" =>
        logger.info("format json")
        results.write.
           format(extension).save(s"${outputPath}Unfiltered")
      case "parquet" =>
        logger.info("format paquet")
        results.write.
          format(extension).save(s"${outputPath}Unfiltered")

      case err: String => logger.error(s"Unrecognised output format $err")
    }
  }
}
