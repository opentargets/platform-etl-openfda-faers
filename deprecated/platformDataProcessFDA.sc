import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object Loaders {

  /** load a drug datasset from OpenTargets drug index dump */
  def loadDrugList(path: String)(implicit ss: SparkSession): DataFrame = {
    val drugList = ss.read
      .json(path)
      .selectExpr("id as chembl_id", "synonyms", "pref_name", "trade_names")
      .withColumn("drug_names",
                  array_distinct(
                    flatten(array(col("trade_names"), array(col("pref_name")), col("synonyms")))))
      .withColumn("_drug_name", explode(col("drug_names")))
      .withColumn("drug_name", lower(col("_drug_name")))
      .select("chembl_id", "drug_name")
      .distinct()

    drugList
  }

  /** generate a target id set from the the drug index dump extracted from
    * mechanisms of action */
  def loadTargetListFromDrugs(path: String)(implicit ss: SparkSession): DataFrame = {
    val tList = ss.read
      .json(path)
      .selectExpr("id as chembl_id", "mechanisms_of_action")
      .withColumn("mechanism_of_action", explode(col("mechanisms_of_action")))
      .withColumn("target_component", explode(col("mechanism_of_action.target_components")))
      .selectExpr("chembl_id", "target_component.ensembl as target_id")
      .distinct()

    tList
  }

  /** load initial OpenFDA FAERS json-lines and preselect needed fields  */
  def loadFDA(path: String)(implicit ss: SparkSession): DataFrame = {
    val fda = ss.read.json(path)

    val columns = Seq("safetyreportid",
                      "serious",
                      "seriousnessdeath",
                      "receivedate",
                      "primarysource.qualification as qualification",
                      "patient")
    fda.selectExpr(columns: _*)
  }

  /** load a blacklist of events you might want to exclude from the computations */
  def loadBlackList(path: String)(implicit ss: SparkSession): DataFrame = {
    val bl = ss.read
      .option("sep", "\t")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .csv(path)

    bl.toDF("reactions")
      .withColumn("reactions", translate(trim(lower(col("reactions"))), "^", "\\'"))
      .orderBy(col("reactions").asc)
  }
}

@main
  def main(drugSetPath: String,
           inputPathPrefix: String,
           outputPathPrefix: String,
           blackListPath: String): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "0")
      .setAppName("similarities-loaders")
      .setMaster("local[*]")

    implicit val ss = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    // AmmoniteSparkSession.sync()

    import ss.implicits._

    /* load the blacklist terms collect as a list and broadcast the field to
    all the cluster nodes thus it can be effectively used per row
     */
    val bl = broadcast(Loaders.loadBlackList(blackListPath))

    // the curated drug list we want
    val targetList = Loaders.loadTargetListFromDrugs(drugSetPath)
    val drugList = Loaders
      .loadDrugList(drugSetPath)
      .join(targetList, Seq("chembl_id"), "left")
      .orderBy(col("drug_name"))
      .cache()

    // load FDA raw lines
    val lines = Loaders.loadFDA(inputPathPrefix)

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
    val wAdverseDrugComb = Window.partitionBy(col("chembl_id"), col("reaction_reactionmeddrapt"))

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

    // write the two datasets on disk as json-lines
    doubleAgg.write.json(outputPathPrefix + "/agg_by_chembl/")
  }
