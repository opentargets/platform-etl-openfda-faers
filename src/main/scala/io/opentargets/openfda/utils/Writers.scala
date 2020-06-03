package io.opentargets.openfda.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

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
          .csv(s"$outputPath/agg_critval_drug_csv/")
        // rename to sensible output if possible.
        val fs = FileSystem
          .get(sparkSession.sparkContext.hadoopConfiguration)
        val fileToRename: Option[FileStatus] = Option(
          fs.globStatus(new Path(s"${fileName}part-00*.csv.gz")).head)
        fileToRename.map(f => {
          fs.rename(f.getPath,
                    new Path(s"${fileName}adverse-events-${DateTime.now.toLocalDate}.csv.gz"))
        })

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
