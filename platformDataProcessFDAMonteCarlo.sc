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
    @tailrec
    def _rmultinom(bin: breeze.stats.distributions.Binomial,
                   size: Int,
                   probV: BDV[Double],
                   binomialList: Array[Int]): Array[Int] = {
      if (binomialList.length >= probV.size)
        binomialList
      else {
        if (binomialList.isEmpty)
          _rmultinom(bin, size, probV, Array(bin.sample))
        else {
          val blSize = binomialList.length
          val P = probV(blSize) / (1D - breeze.linalg.sum(probV(0 until blSize)))
          val N = math.abs(size - binomialList.sum)

          val Bin = N match {
            case 0 => None
            case _ => Some(breeze.stats.distributions.Binomial(N, P))
          }

          _rmultinom(bin, size, probV, binomialList :+ Bin.map(_.sample).getOrElse(0))
        }
      }
    }

    require(probV.size > 0 && size > 0, "the probability vector must be > 0 and the size > 0")

    val X: BDM[Double] = BDM.zeros(probV.size, n)
    val p = probV / breeze.linalg.max(probV)

    val Bin = breeze.stats.distributions.Binomial(size, p(0))
    for (i <- 0 until n) {
      val sampleIter: Array[Int] = Array()
      val sample: BDV[Int] = BDV(_rmultinom(Bin, size, p, sampleIter))
      X(::, i) := breeze.linalg.convert(sample, Double)
    }

    X
  }

}

object Loaders {
  def loadAggFDA(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.json(path)
}

@main
  def main(inputPath: String, outputPathPrefix: String): Unit = {
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

        val t0 = System.nanoTime()
        x := MathHelpers.rmultinom(permutations, n_j, probV)
        val t1 = System.nanoTime()
//        println(s"done computing rmultinom ${(t1 - t0)/1000000}")
        val B = x - A
        val C = z - A
        val D = N - z - x + A

        val aterm = A * (math.log(A) - breeze.numerics.log(A +:+ B))
        val cterm = C * (math.log(C) - breeze.numerics.log(C +:+ D))
        val acterm = (A + C) * (math.log(A + C) - breeze.numerics.log((A +:+ B) + (C +:+ D)))
        val llr: BDM[Double] = aterm +:+ cterm -:- acterm

        llr(llr.findAll(e => e.isNaN || e.isInfinity)) := 0.0

        val llrs = breeze.linalg.max(llr(::, *))

        val t2 = System.nanoTime()
//        println(s"llrs ${(t2 - t1)/1000000} size ${llrs.t.size}")
        val critVal = DescriptiveStats.percentile(llrs.t.activeValuesIterator, prob)

        critVal
      })

    val critVal = fdas
//      .where($"chembl_id" === "CHEMBL714")
      .withColumn("uniq_reports_total", $"A" + $"B" + $"C" + $"D")
      .groupBy($"chembl_id")
      .agg(
        first($"uniq_report_ids_by_drug").as("uniq_report_ids_by_drug"),
        collect_list($"uniq_report_ids_by_reaction").as("n_i"),
        first($"uniq_reports_total").as("uniq_reports_total"),
        first($"A").as("uniq_report_ids")
      )
      .withColumn("critVal",
                  udfProbVector(lit(100),
                                $"uniq_report_ids_by_drug",
                                $"n_i",
                                $"uniq_reports_total",
                                $"uniq_report_ids",
                                lit(0.99)))
      .persist(StorageLevel.DISK_ONLY)

    fdas
      .join(critVal.select("chembl_id", "critVal"), Seq("chembl_id"), "inner")
      .where(col("llr") > col("critVal"))
      .write
      .json(outputPathPrefix + "/agg_critval/")
  }