package io.opentargets.openfda.config

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigSource
import pureconfig.generic.auto.{getClass, _}

case class OTConfig(sparkUri: Option[String], common: Common, fda: Fda)
case class Common(defaultSteps: Seq[String], output: String)
case class FdaInputs(
    chemblData: String,
    fdaData: String,
    blacklist: Option[String]
) {
  require(chemblData.endsWith("json"))
  require(fdaData.endsWith("jsonl"))
  require(if (blacklist.isDefined) blacklist.get.endsWith(".txt") else true)
}
case class MonteCarlo(permutations: Int, percentile: Double) {
  require(permutations > 0)
  require(percentile > 0 && percentile < 1)
}
case class Fda(montecarlo: MonteCarlo, fdaInputs: FdaInputs, outputs: Seq[String]) {
  private def validOutput(str: String): Boolean = List("csv", "json", "jsonl").contains(str)
  require(outputs.forall(validOutput))
}
object Configuration extends LazyLogging {
  lazy val config: OTConfig = load
  // fallback for if user doesn't supply blacklist since it shouldn't change often
  val defaultBlacklistPath: String = this.getClass.getResource("/blacklisted_events.txt").getPath

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
