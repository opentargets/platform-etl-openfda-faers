package io.opentargets.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.openfda.Main.Config
import io.opentargets.openfda.utils.Loaders
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OpenFdaEtl extends LazyLogging {

  def run(config: Config)(implicit ss: SparkSession): DataFrame = {

    import ss.implicits._

    // load inputs
    /* load the blacklist terms collect as a list and broadcast the field to
        all the cluster nodes thus it can be effectively used per row
     */
    val bl = broadcast(Loaders.loadBlackList(config.blacklist.getAbsolutePath))

    // the curated drug list we want
    val targetList = Loaders.loadTargetListFromChemblDrugs(config.chemblJson.getAbsolutePath)
    val drugList = Loaders
      .loadChemblDrugList(config.chemblJson.getAbsolutePath)
      .join(targetList, Seq("chembl_id"), "left")
      .orderBy(col("drug_name"))
      .cache()

    // load FDA raw lines
    val lines = Loaders.loadFDA(config.fdaPath.getAbsolutePath)
    val fdasF = lines
      .withColumn("reaction", explode(col("patient.reaction")))
      // after explode this we will have reaction-drug pairs
      .withColumn("drug", explode(col("patient.drug")))
      // just the fields we want as columns
      .selectExpr(
        "safetyreportid",
        "serious",
        "receivedate",
        "ifnull(seriousnessdeath, '0') as seriousness_death",
        "qualification",
        "trim(translate(lower(reaction.reactionmeddrapt), '^', '\\'')) as reaction_reactionmeddrapt",
        "ifnull(lower(drug.medicinalproduct), '') as drug_medicinalproduct",
        "ifnull(drug.openfda.generic_name, array()) as drug_generic_name_list",
        "ifnull(drug.openfda.brand_name, array()) as drug_brand_name_list",
        "ifnull(drug.openfda.substance_name, array()) as drug_substance_name_list",
        "drug.drugcharacterization as drugcharacterization"
      )
      // we dont need these columns anymore
      .drop("patient", "reaction", "drug", "_reaction", "seriousnessdeath")
      // delicated filter which should be looked at FDA API to double check
      .where(col("qualification")
        .isInCollection(Seq("1", "2", "3")) and col("drugcharacterization") === "1")
      // drug names comes in a large collection of multiple synonyms but it comes spread across multiple fields
      .withColumn(
        "drug_names",
        array_distinct(
          concat(col("drug_brand_name_list"),
                 array(col("drug_medicinalproduct")),
                 col("drug_generic_name_list"),
                 col("drug_substance_name_list")))
      )
      // the final real drug name
      .withColumn("_drug_name", explode(col("drug_names")))
      .withColumn("drug_name", lower(col("_drug_name")))
      // rubbish out
      .drop("drug_generic_name_list", "drug_substance_name_list", "_drug_name")
      .where($"drug_name".isNotNull and $"reaction_reactionmeddrapt".isNotNull and
        $"safetyreportid".isNotNull and $"seriousness_death" === "0" and
        $"drug_name" =!= "")

    // remove blacklisted reactions using a left_anti which is the complement of
    // left_semi so the ones from the left side which are not part of the equality
    val fdasFiltered = fdasF
      .join(bl, fdasF("reaction_reactionmeddrapt") === bl("reactions"), "left_anti")

    val wAdverses = Window.partitionBy(col("reaction_reactionmeddrapt"))
    val wDrugs = Window.partitionBy(col("chembl_id"))
    val wAdverseDrugComb =
      Window.partitionBy(col("chembl_id"), col("reaction_reactionmeddrapt"))

    // and we will need this processed data later on
    val fdas = fdasFiltered
      .join(drugList, Seq("drug_name"), "inner")
      .withColumn("uniq_report_ids_by_reaction",
                  approx_count_distinct(col("safetyreportid")).over(wAdverses))
      .withColumn("uniq_report_ids_by_drug",
                  approx_count_distinct(col("safetyreportid")).over(wDrugs))
      .withColumn("uniq_report_ids",
                  approx_count_distinct(col("safetyreportid")).over(wAdverseDrugComb))
      .select(
        "safetyreportid",
        "chembl_id",
        "reaction_reactionmeddrapt",
        "uniq_report_ids_by_reaction",
        "uniq_report_ids_by_drug",
        "uniq_report_ids"
      )

    // total unique report ids
    val uniqReports = fdas.select("safetyreportid").distinct.count

    // compute llr and its needed terms as per drug-reaction pair
    val doubleAgg = fdas
      .drop("safetyreportid")
      .withColumnRenamed("uniq_report_ids", "A")
      .withColumn("C", col("uniq_report_ids_by_drug") - col("A"))
      .withColumn("B", col("uniq_report_ids_by_reaction") - col("A"))
      .withColumn("D",
                  lit(uniqReports) - col("uniq_report_ids_by_drug") - col(
                    "uniq_report_ids_by_reaction") + col("A"))
      .withColumn("aterm", $"A" * (log($"A") - log($"A" + $"B")))
      .withColumn("cterm", $"C" * (log($"C") - log($"C" + $"D")))
      .withColumn("acterm", ($"A" + $"C") * (log($"A" + $"C") - log($"A" + $"B" + $"C" + $"D")))
      .withColumn("llr", $"aterm" + $"cterm" - $"acterm")
      .distinct()
      .where($"llr".isNotNull and !$"llr".isNaN)

    doubleAgg

  }

}
