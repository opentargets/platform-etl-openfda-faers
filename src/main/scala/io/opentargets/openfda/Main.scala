package io.opentargets.openfda

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends LazyLogging {

  def main(args: Array[String]): Unit = {
    ETL(args)

  }

}
