package io.opentargets.openfda

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import scopt.OptionParser

object OpenFdaEtl extends LazyLogging {

  def main(args: Array[String]) = {
    parser.parse(args, Config()) match {
      case Some(c) => {
        logger.info(
          s"""
             |ChEMBL database: ${c.drugSetPath}
             |FDA database: ${c.inputPathPrefix}
             |Output: ${c.outputPathPrefix}
             |""".stripMargin)

        // set up spark context

        // load inputs

        //
      }
      case _ => sys.exit(1)
    }

  }

  val parser = new OptionParser[Config]("openFdaEtl") {
    head("OpenFda Etl pipeline", "0.1")

    opt[File]("drugSetPath").required
      .valueName("<directory>")
      .action((x, c) => c.copy(drugSetPath = x))
      .validate(exists)
      .validate(readable)
      .validate(containsJson)
      .text("Directory containing ChEMBL drug database")

    opt[File]("inputPathPrefix").required
      .valueName("<directory>")
      .action((x, c) => c.copy(inputPathPrefix = x))
      .validate(exists)
      .validate(readable)
      .validate(containsJson)
      .text("Directory containing FDA database json files")

    opt[File]("outputPathPrefix").required
      .valueName("<directory>")
      .action((x, c) => c.copy(outputPathPrefix = x))
      .validate(exists)
      .validate(writeable)
      .text("Directory to write output files")

    def exists: File => Either[String, Unit] =
      x =>
        if (x.exists) success
        else failure(s"Directory ${x.getAbsolutePath} does not exist")
    def readable: File => Either[String, Unit] =
      x =>
        if (x.canRead) success
        else failure(s"Can not read directory: ${x.getAbsolutePath}")
    def writeable: File => Either[String, Unit] =
      x =>
        if (x.canWrite) success
        else failure(s"Can not write to directory: ${x.getAbsolutePath}")
    def containsJson: File => Either[String, Unit] = x =>
        if (x.listFiles.exists(f => f.getName.endsWith("json") || f.getName.endsWith("jsonl")) ) success
        else failure(s"No json files found in directory: ${x.getAbsolutePath}")
  }

  case class Config(drugSetPath: File = new File(""),
                    inputPathPrefix: File = new File(""),
                    outputPathPrefix: File = new File(""))

}
