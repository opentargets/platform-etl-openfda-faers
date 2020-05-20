package io.opentargets.openfda.config

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigSource
import pureconfig.generic.auto._

case class OTConfig(sparkUri: Option[String], common: Common, montecarlo: MonteCarlo)
case class Common(defaultSteps: Seq[String], inputs: Inputs, output: String, outputFormat: String)
case class Inputs(
    blacklist: InputInfo,
    chemblData: InputInfo,
    fdaData: InputInfo
)
case class InputInfo(format: String, path: String)
case class MonteCarlo(permutations: Int, percentile: Double) {
  require(permutations > 0)
  require(percentile > 0 && percentile < 1)
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
