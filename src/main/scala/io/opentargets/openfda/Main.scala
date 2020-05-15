package io.opentargets.openfda

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.openfda.Main.Config
import io.opentargets.openfda.stage.{MonteCarloSampling, OpenFdaEtl}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

object Main extends LazyLogging {

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(c) =>
        logger.info(s"""
                       |ChEMBL database: ${c.chemblJson}
                       |FDA database: ${c.fdaPath}
                       |Output: ${c.outputPath}
                       |""".stripMargin)

        // set up spark context
        logger.info("Setting up Spark context")
        val sparkConf = new SparkConf()
          .set("spark.driver.maxResultSize", "0")
          .setAppName("similarities-loaders")
          .setMaster("local[*]")

        implicit val ss: SparkSession = SparkSession.builder
          .config(sparkConf)
          .getOrCreate

        logger.info("Beginning pipeline...")
        val doubleAgg: DataFrame = OpenFdaEtl.run(c)
        ResultWriters.writeFdaResults(doubleAgg, c)

        val monteCarloResults = MonteCarloSampling.run(doubleAgg)
        ResultWriters.writeMonteCarloResults(monteCarloResults, c.outputPath.getAbsolutePath)

      case _ => sys.exit(1)
    }

  }

  val parser: OptionParser[Config] = new OptionParser[Config]("openFdaEtl") {
    head("OpenFda Etl pipeline", "0.1")
    import CliValidators._
    opt[File]("chemblData").required
      .valueName("<file>")
      .action((x, c) => c.copy(chemblJson = x))
      .validate(exists)
      .validate(readable)
      .validate(isA(_, "json"))
      .text("Directory containing ChEMBL drug database")

    opt[File]("fdaData").required
      .valueName("<directory>")
      .action((x, c) => c.copy(fdaPath = x))
      .validate(exists)
      .validate(readable)
      .validate(containsJson)
      .text("Directory containing FDA database json files")

    opt[File]("outputPath").required
      .valueName("<directory>")
      .action((x, c) => c.copy(outputPath = x))
      .validate(exists)
      .validate(writeable)
      .text("Directory to write output files")

    opt[File]("blacklist").required
      .valueName("<file.txt>")
      .action((x, c) => c.copy(blacklist = x))
      .validate(exists)
      .validate(readable)
      .validate(isA(_, "txt"))
      .text("Blacklist of excluded events")

  }
  // todo: add optional configuration for MC
  case class Config(chemblJson: File = new File(""),
                    fdaPath: File = new File(""),
                    outputPath: File = new File(""),
                    blacklist: File = new File(""))

}

object CliValidators {
  def exists: File => Either[String, Unit] =
    x =>
      if (x.exists) Right()
      else Left(s"Directory or File ${x.getAbsolutePath} does not exist")
  def readable: File => Either[String, Unit] =
    x =>
      if (x.canRead) Right()
      else Left(s"Can not read: ${x.getAbsolutePath}")
  def writeable: File => Either[String, Unit] =
    x =>
      if (x.canWrite) Right()
      else Left(s"Can not write to directory: ${x.getAbsolutePath}")
  def containsJson: File => Either[String, Unit] =
    x =>
      if (x.listFiles.exists(f => f.getName.endsWith("json") || f.getName.endsWith("jsonl")))
        Right()
      else Left(s"No json files found in directory: ${x.getAbsolutePath}")
  def isJson: File => Either[String, Unit] =
    x =>
      if (x.isFile && x.getName.endsWith("json")) Right()
      else Left(s"${x.getName} is not a json file!")
  def isTextFile: File => Either[String, Unit] =
    x =>
      if (x.isFile && x.getName.endsWith("txt")) Right()
      else Left(s"${x.getName} is not a txt file!")

  def isA(file: File, fileType: String): Either[String, Unit] =
    if (file.isFile && file.getName.endsWith(fileType)) Right()
    else Left(s"${file.getName} is not a $fileType file!")
}

object ResultWriters extends LazyLogging {
  def writeMonteCarloResults(results: DataFrame, outputPath: String): Unit = {
    logger.info("Writing results of monte carlo sampling...")
    results.write
      .json(outputPath + s"$outputPath/agg_critval_drug/")

    // write to one single compressed file
    results
      .coalesce(1)
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"$outputPath/agg_critval_drug_csv/")
  }

  def writeFdaResults(results: DataFrame, config: Config): Unit = {
    logger.info("Writing results of fda data transformation...")
    results.write.json(s"${config.outputPath.getAbsolutePath}/agg_by_chembl/")
  }
}
