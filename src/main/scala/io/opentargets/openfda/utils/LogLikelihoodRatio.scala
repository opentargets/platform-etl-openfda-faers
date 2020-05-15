package io.opentargets.openfda.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, col, lit, log}

object LogLikelihoodRatio {

  case class LLROptions(columnsPrefix: String,
                        llrColName: String,
                        criticalValueColname: String,
                        monteCarloPermutations: Int,
                        monteCarloPercentile: Double) {

    val AName = columnsPrefix + "A"
    val BName = columnsPrefix + "B"
    val CName = columnsPrefix + "C"
    val DName = columnsPrefix + "D"

    val atermName = columnsPrefix + "aterm"
    val ctermName = columnsPrefix + "cterm"
    val actermName = columnsPrefix + "acterm"

    val llrName = columnsPrefix + llrColName

    val A = col(AName)
    val B = col(BName)
    val C = col(CName)
    val D = col(DName)

    val aterm = col(atermName)
    val cterm = col(ctermName)
    val acterm = col(actermName)

    val llr = col(llrName)
  }

  val defaultLogLikelyhoodRationOptions = LLROptions(
    "llr_",
    "value",
    "critical_value",
    1000,
    0.95
  )
  implicit class Implicits(source: DataFrame) {
    def llr(documentId: String, objectId: String, subjectId: String)(
        options: LLROptions = defaultLogLikelyhoodRationOptions): DataFrame = {

      //      val uniqCountByDocumentId = temporalColumnsPrefix + documentId + "_uniq_count"
      val uniqCountByObjectId = options.columnsPrefix + objectId + "_uniq_count"
      val uniqCountBySubjectId = options.columnsPrefix + subjectId + "_uniq_count"
      val uniqCountByObjectSubjectId = options.columnsPrefix + objectId + "_" + subjectId + "_uniq_count"

      val wSubjects = Window.partitionBy(col(subjectId))
      val wObjects = Window.partitionBy(col(objectId))
      val wObjectSubjectPairs =
        Window.partitionBy(col(objectId), col(subjectId))

      // total unique documents by documentId
      val uniqTotalCountsByDocumentId = source.select(documentId).distinct.count

      // compute counts for the contingency table
      val df = source
        .withColumn(uniqCountByObjectId, approx_count_distinct(col(documentId)).over(wObjects))
        .withColumn(uniqCountBySubjectId, approx_count_distinct(col(documentId)).over(wSubjects))
        .withColumn(uniqCountByObjectSubjectId,
                    approx_count_distinct(col(documentId)).over(wObjectSubjectPairs))

      // compute llr and its needed terms as per object-subject pair
      df.withColumnRenamed(uniqCountByObjectSubjectId, options.AName)
        .withColumn(options.CName, col(uniqCountByObjectId) - options.A)
        .withColumn(options.BName, col(uniqCountBySubjectId) - options.A)
        .withColumn(options.DName,
                    lit(uniqTotalCountsByDocumentId) - col(uniqCountByObjectId) - col(
                      uniqCountBySubjectId) + options.A)
        .withColumn(options.atermName, options.A * (log(options.A) - log(options.A + options.B)))
        .withColumn(options.ctermName, options.C * (log(options.C) - log(options.C + options.D)))
        .withColumn(options.actermName,
                    (options.A + options.C) * (log(options.A + options.C) - log(
                      options.A + options.B + options.C + options.D)))
        .withColumn(options.llrName, options.aterm + options.cterm - options.acterm)
    }

    /** after llr computation null or nan results are not filtered out as you might want
      * to inspect further those results. After that, just call this filter
      */
    def filterValidLLRRows(options: LLROptions = defaultLogLikelyhoodRationOptions): DataFrame =
      source.where(options.llr.isNotNull and !options.llr.isNaN)

    /** compute the critical value and it already apply the method filterValidLLRRows
      * before computation
      */
    def criticalValue(objectId: String, subjectId: String)(
        options: LLROptions = defaultLogLikelyhoodRationOptions): DataFrame = {
      // create synthetic data from permutations
      val llrs = filterValidLLRRows(options)
      llrs
    }

  }
}
