package io.opentargets.openfda.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Loaders extends LazyLogging {

  /** load a drug dataset from OpenTargets drug index dump */
  def loadChemblDrugList(path: String)(implicit ss: SparkSession): DataFrame = {

    logger.info("Loading ChEMBL drug list...")
    val drugList = ss.read
      .json(path)
      .selectExpr("id as chembl_id",
                  "synonyms as synonyms",
                  "pref_name pref_name",
                  "trade_names as trade_names")
      .withColumn("drug_names",
                  array_distinct(
                    flatten(array(col("trade_names"), array(col("pref_name")), col("synonyms")))))
      .withColumn("_drug_name", explode(col("drug_names")))
      .withColumn("drug_name", lower(col("_drug_name")))
      .select("chembl_id", "drug_name")
      .distinct()

    drugList
  }

  /** load initial OpenFDA FAERS json-lines and preselect needed fields
    * @param path file directory containing raw-fda data
    * @return fda data with only the columns needed for pipeline analysis
    */
  def loadFDA(path: String)(implicit ss: SparkSession): DataFrame = {

    logger.info("Loading FDA database json...")

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

    logger.info("Loading event blacklist...")

    val bl = ss.read
      .option("sep", "\t")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .csv(path)

    bl.toDF("reactions")
      .withColumn("reactions", translate(trim(lower(col("reactions"))), "^", "\\'"))
      .orderBy(col("reactions").asc)
  }

  /*
   * The MedDRA raw data comes in a $ separated value format. There are large number of null fields that we're not
   * interested which we want to discard. This method cleans the data and returns a usable dataframe.
   * @return DataFrame with columns: ptCode, ptName. Code is meddra identifier, ptName is reaction in Fda data.
   */
  def loadMeddraPreferredTerms(path: String)(implicit ss: SparkSession): DataFrame = {
    val cols = Seq("ptCode", "ptName").zipWithIndex
    val meddraRaw = ss.read.csv(path)
    meddraRaw
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$+", ","))
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$$", ""))
      .withColumn("_c0", split(col("_c0"), ","))
      .select(cols.map(i => col("_c0").getItem(i._2).as(s"${i._1}")): _*)
      .withColumn("ptName", lower(col("ptName")))
  }

}
