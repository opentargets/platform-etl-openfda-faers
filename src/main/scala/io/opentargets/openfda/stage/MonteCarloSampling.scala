package io.opentargets.openfda.stage

import io.opentargets.openfda.config.ETLSessionContext
import io.opentargets.openfda.utils.MathUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, first, lit, udf}

object MonteCarloSampling {

  // To enabling running as part of pipeline
  def apply(inputDf: DataFrame, percentile: Double = 0.99, permutations: Int = 100)(
      implicit context: ETLSessionContext): DataFrame = {

    import context.sparkSession.implicits._

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
      // critVal_drug is created using the MonteCarlo method to use a binomial distribution
      // for that particular drug.
      .withColumn("critVal_drug",
                  udfProbVector(lit(permutations),
                                $"uniq_report_ids_by_drug",
                                $"n_i",
                                $"uniq_reports_total",
                                lit(percentile)))
      .select("chembl_id", "critVal_drug")

    val exprs = List(
      "chembl_id",
      "reaction_reactionmeddrapt as event",
      "A as count",
      "llr",
      "critVal_drug as critval",
      "meddraCode"
    )

    val filteredDF = inputDf
      .join(critValDrug, Seq("chembl_id"), "inner")
      .where(($"llr" > $"critVal_drug") and
        ($"critVal_drug" > 0))
      .selectExpr(exprs: _*)

    filteredDF

  }

}
