package io.opentargets.openfda.stage

import io.opentargets.openfda.ResultWriters
import io.opentargets.openfda.utils.{Loaders, MathUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{collect_list, first, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MonteCarloSampling {

  // To enabling running as part of pipeline
  def run(inputDf: DataFrame, percentile: Double = 0.99, permutations: Int = 100)(
      implicit sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._

    // Register function with Spark
    val udfCriticalValues: (Int, Int, Seq[Long], Int, Double) => Double =
      MathUtils.calculateCriticalValues
    val udfProbVector = udf(udfCriticalValues)

    // calculate critical values using UDF
    val critValDrug = inputDf
      .withColumn("uniq_reports_total", $"A" + $"B" + $"C" + $"D")
      .withColumn("uniq_report_ids", $"A")
      .groupBy($"chembl_id")
      .agg(
        first($"uniq_reports_total").as("uniq_reports_total"),
        first($"uniq_report_ids").as("uniq_report_ids"),
        collect_list($"uniq_report_ids_by_reaction").as("n_i"),
        first($"uniq_report_ids_by_drug").as("uniq_report_ids_by_drug"),
      )
      .withColumn("critVal_drug",
                  udfProbVector(lit(permutations),
                                $"uniq_report_ids_by_drug",
                                $"n_i",
                                $"uniq_reports_total",
                                $"uniq_report_ids",
                                lit(percentile)))
      .select("chembl_id", "critVal_drug")

    val exprs = List(
      "chembl_id",
      "reaction_reactionmeddrapt as event",
      "A as report_count",
      "llr",
      "critVal_drug as critval"
    )

    val filteredDF = inputDf
      .join(critValDrug, Seq("chembl_id"), "inner")
      .where(($"llr" > $"critVal_drug") and
        ($"critVal_drug" > 0))
      .selectExpr(exprs: _*)

    filteredDF

  }

  // To enabling running as a stand-alone file
  def main(inputPath: String,
           outputPathPrefix: String,
           percentile: Double = 0.99,
           permutations: Int = 100): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "0")
      .setAppName("similarities-loaders")
      .setMaster("local[*]")

    implicit val ss: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    import ss.implicits._

    val fdas = Loaders.loadAggFDA(inputPath)

    val results = run(fdas, percentile, permutations)

    ResultWriters.writeMonteCarloResults(results, outputPathPrefix)

  }

}
