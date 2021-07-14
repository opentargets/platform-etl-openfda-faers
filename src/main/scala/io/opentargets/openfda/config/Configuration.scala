package io.opentargets.openfda.config

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigSource
import pureconfig.generic.auto._

case class OTConfig(sparkUri: Option[String], common: Common, fda: Fda)

case class Common(defaultSteps: Seq[String], output: String)

case class FdaInputs(
                      blacklist: String,
                      chemblData: String,
                      fdaData: String,
                      meddraRelease: Option[String]
                    ) {
  require(blacklist.endsWith("txt"), "Blacklist is required in txt extension")
  require(chemblData.endsWith("json"), "ChEMBL data is required in json format")
  require(fdaData.endsWith("json") || fdaData.endsWith("jsonl"),
    "FDA data is required in jsonl format")
}

case class Sampling(output: String, size: Double = 0.1, enabled: Boolean = false) {
  require(size > 0)
}

case class MonteCarlo(permutations: Int, percentile: Double) {
  require(permutations > 0)
  require(percentile > 0 && percentile < 1)
}

case class Fda(montecarlo: MonteCarlo,
               fdaInputs: FdaInputs,
               outputs: Seq[String],
               sampling: Sampling) {
  private def validOutput(str: String): Boolean = List("csv", "json", "jsonl", "parquet").contains(str)

  require(outputs.forall(validOutput))
}

object Configuration extends LazyLogging {
  lazy val config: OTConfig = load

  // throwing an error on initialisation rather than returning result so job fails on startup
  // rather than trying to save the situation.
  private def load: OTConfig = {
    logger.info("load configuration from file")
    val config = ConfigFactory.load()
    val obj = ConfigSource.fromConfig(config).loadOrThrow[OTConfig]
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}
