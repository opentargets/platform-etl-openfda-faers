import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.5`
import $ivy.`org.apache.spark::spark-mllib:2.4.5`
import $ivy.`org.apache.spark::spark-sql:2.4.5`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vectors}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.stats.distributions.RandBasis
import breeze.linalg._

import scala.annotation.tailrec
import scala.util.Try

object MathHelpers {

  /** rmultinom(n, size, prob) from R
    *
    * @link https://stat.ethz.ch/R-manual/R-devel/library/stats/html/Multinom.html
    *      Here the implementation reference
    */
  def rmultinom(n: Int, size: Int, probV: BDV[Double]): BDM[Double] = {
    require(probV.size > 0 && size > 0, "the probability vector must be > 0 and the size > 0")

    val X: BDM[Double] = BDM.zeros(probV.size, n)
    val p = probV / breeze.linalg.sum(probV)

    val Bin = breeze.stats.distributions.Binomial(size, p(0))
    for (i <- 0 until n) {
      // get first sample each permutation
      X(0, i) = Bin.sample().toDouble

      for (j <- 1 until p.size) {
        val P = p(j) / (1D - breeze.linalg.sum(p(0 until j)))
        val N = math.abs(size - breeze.linalg.sum(X(0 until j, i))).toInt

        val Binj = N match {
          case 0 => 0
          case _ => breeze.stats.distributions.Binomial(N, P).sample
        }

        X(j, i) = Binj.toDouble
      }
    }

    X
  }

}

object Loaders {
  def loadAggFDA(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.json(path)
}

@main
  def main(inputPath: String,
           outputPathPrefix: String,
           percentile: Double = 0.99,
           permutations: Int = 100): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "0")
      .setAppName("similarities-loaders")
      .setMaster("local[*]")

    implicit val ss = SparkSession.builder
      .config(sparkConf)
      .getOrCreate

    import ss.implicits._

    val fdas = Loaders.loadAggFDA(inputPath)

    /**
      * multinomial with parameter n_j for all n_i / n
      * n - total number of reports
      * prob - top percentile in term of probability (0,1)
      * n_i - vector of the counts of reports of each adverse (i) event per drug
      * n_j - the counts of reports of the drug j
      * permutations - number of times per drug (j)
      */
    val udfProbVector = udf(
      (permutations: Int, n_j: Int, n_i: Seq[Long], n: Int, n_ij: Long, prob: Double) => {
        import breeze.linalg._
        import breeze.stats._
        val z = n_j.toDouble
        val A = n_ij.toDouble
        val N = n.toDouble
        val y = convert(BDV(n_i.toArray), Double)
        val probV = y / N
        val x: BDM[Double] = BDM.zeros(probV.size, permutations)

        x := MathHelpers.rmultinom(permutations, n_j, probV)

        //  myLLRs <- t(sapply(1:length(Pvector), function(i){
        //    logLRnum(Simulatej[i, ], n_i[i], n_j, n)
        //  }))
        //  myLLRs <- myLLRs - n_j * log(n_j) + n_j * log(n)
        //
        // logLRnum<-function(x, y, z, n){
        //  logLR <- x * (log(x) - log(y)) + (z-x) * (log(z - x) - log(n - y))
        //  return(logLR)
        //}

        val LLRS: BDM[Double] = BDM.zeros(permutations, probV.size)

        for (c <- 0 until probV.size) {
          val X = x(c, ::).t
          val lX = breeze.numerics.log(X)
          val ly = math.log(y(c))
          val lzX = breeze.numerics.log(z - X)
          val XX = X *:* (lX - ly) + (z - X) *:* (lzX - math.log(N - y(c)))
          LLRS(::, c) := XX
        }

        LLRS := LLRS - z * math.log(z) + z * math.log(N)
        LLRS(LLRS.findAll(e => e.isNaN || e.isInfinity)) := 0.0
        val maxLLRS = breeze.linalg.max(LLRS(::, *))
        val critVal = DescriptiveStats.percentile(maxLLRS.t.data, prob)

//        val B = x - A
//        val C = z - A
//        val D = N - z - x + A
//
//        val aterm = A * (math.log(A) - breeze.numerics.log(A +:+ B))
//        val cterm = C * (math.log(C) - breeze.numerics.log(C +:+ D))
//        val acterm = (A + C) * (math.log(A + C) - breeze.numerics.log((A +:+ B) + (C +:+ D)))
//        val llr: BDM[Double] = aterm +:+ cterm -:- acterm
//
//        llr(llr.findAll(e => e.isNaN || e.isInfinity)) := 0.0
//        val llrs = breeze.linalg.max(llr(::, *))
//        val critVal = DescriptiveStats.percentile(llrs.t.activeValuesIterator, prob)

        critVal
      })

    val critValDrug = fdas
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
      .persist(StorageLevel.DISK_ONLY)

    fdas
      .join(critValDrug, Seq("chembl_id"), "inner")
      .where(col("llr") > col("critVal_drug"))
      .write
      .json(outputPathPrefix + "/agg_critval_drug/")
  }