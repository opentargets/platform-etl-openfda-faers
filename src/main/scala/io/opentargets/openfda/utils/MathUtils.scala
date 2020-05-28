package io.opentargets.openfda.utils

import breeze.linalg.{sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.Binomial

object MathUtils {

  def calculateCriticalValues(permutations: Int,
                              n_j: Int,
                              n_i: Seq[Long],
                              total: Int,
                              prob: Double): Double = {
    import breeze.linalg._
    import breeze.stats._

    val drug_count = n_j.toDouble
    val N = total.toDouble
    val y = convert(BDV(n_i.toArray), Double)
    val probV = y / N
    val x: BDM[Double] = BDM.zeros(probV.size, permutations)

    x := MathUtils.rmultinom(permutations, n_j, probV)

    val LLRS: BDM[Double] = BDM.zeros(probV.size, permutations)

    for (c <- 0 until probV.size) {
      val X = x(c, ::).t
      val lX = breeze.numerics.log(X)
      val ly = math.log(y(c))
      val lzX = breeze.numerics.log(drug_count - X)
      val XX = X *:* (lX - ly) + (drug_count - X) *:* (lzX - math.log(N - y(c)))
      LLRS(c, ::) := XX.t
    }

    LLRS := LLRS - drug_count * math.log(drug_count) + drug_count * math.log(N)
    LLRS(LLRS.findAll(e => e.isNaN || e.isInfinity)) := 0.0
    val maxLLRS = breeze.linalg.max(LLRS(::, *))
    val critVal = DescriptiveStats.percentile(maxLLRS.t.data, prob)

    critVal
  }

  /** rmultinom(n, size, prob) from R
    *
    * @link https://stat.ethz.ch/R-manual/R-devel/library/stats/html/Multinom.html
    *       Here the implementation reference
    *       and here https://github.com/wch/r-source/blob/f8d4d7d48051860cc695b99db9be9cf439aee743/src/nmath/rmultinom.c
    */
  def rmultinom(iterations: Int, size: Int, probV: BDV[Double]): BDM[Double] = {
    require(probV.size > 0 && size > 0, "the probability vector must be > 0 and the size > 0")
    require(iterations > 0, "iterations must be greater than zero.")

    val X: BDM[Double] = BDM.zeros(probV.size, iterations)
    val p: BDV[Double] = probV / sum(probV)

    val Bin = Binomial(size, p(0))
    for (i <- 0 until iterations) {

      X(0, i) = Bin.sample.toDouble

      for (j <- 1 until p.size) {
        val P = p(j) / (1D - sum(p(0 until j)))
        val N = (size - sum(X(0 until j, i))).toInt

        val Binj: Int = N match {
          case n if n <= 0 => 0
          case _           => Binomial(N, P).sample
        }

        X(j, i) = Binj.toDouble
      }
    }

    X
  }

}
