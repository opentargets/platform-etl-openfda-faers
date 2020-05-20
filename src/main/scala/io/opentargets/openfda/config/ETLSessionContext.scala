package io.opentargets.openfda.config

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class ETLSessionContext(configuration: OTConfig, sparkSession: SparkSession)

object ETLSessionContext extends LazyLogging {
  val progName: String = "ot-platform-fda-pipeline"
  val config: OTConfig = Configuration.config

  def apply(): ETLSessionContext =
    ETLSessionContext(config, getOrCreateSparkSession(progName, config.sparkUri))

  def getOrCreateSparkSession(appName: String, sparkUri: Option[String]): SparkSession = {
    logger.info(s"create spark session with uri:'${sparkUri.toString}'")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.driver.maxResultSize", "0")
      .set("spark.debug.maxToStringFields", "2000")

    // if some uri then set master must be set otherwise
    // it tries to get from env if any yarn running
    val conf = sparkUri match {
      case Some(uri) if uri.nonEmpty => sparkConf.setMaster(uri)
      case _                         => sparkConf
    }

    SparkSession.builder
      .config(conf)
      .getOrCreate
  }
}
