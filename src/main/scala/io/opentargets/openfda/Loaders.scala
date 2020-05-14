package io.opentargets.openfda

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Loaders extends LazyLogging {

  private val blacklistPath: String = getClass.getResource("/blacklisted_events.txt").getPath

  /** load a drug dataset from OpenTargets drug index dump */
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
  def loadBlackList(implicit ss: SparkSession): DataFrame = {

    logger.info("Loading event blacklist...")

    val bl = ss.read
      .option("sep", "\t")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .csv(blacklistPath)

    bl.toDF("reactions")
      .withColumn("reactions", translate(trim(lower(col("reactions"))), "^", "\\'"))
      .orderBy(col("reactions").asc)
  }
}

