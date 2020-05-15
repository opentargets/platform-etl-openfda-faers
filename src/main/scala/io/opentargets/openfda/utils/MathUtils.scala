package io.opentargets.openfda.utils

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

object MathUtils {
  // todo: write some tests and possible refactor
  val fun: (Int, Int, Seq[Long], Int, Long, Double) => Double =
    (permutations: Int, n_j: Int, n_i: Seq[Long], n: Int, n_ij: Long, prob: Double) => {
      import breeze.linalg._
      import breeze.stats._
      val z = n_j.toDouble
      val A = n_ij.toDouble
      val N = n.toDouble
      val y = convert(BDV(n_i.toArray), Double)
      val probV = y / N
      val x: BDM[Double] = BDM.zeros(probV.size, permutations)

      x := MathUtils.rmultinom(permutations, n_j, probV)

      val LLRS: BDM[Double] = BDM.zeros(probV.size, permutations)

      for (c <- 0 until probV.size) {
        val X = x(c, ::).t
        val lX = breeze.numerics.log(X)
        val ly = math.log(y(c))
        val lzX = breeze.numerics.log(z - X)
        val XX = X *:* (lX - ly) + (z - X) *:* (lzX - math.log(N - y(c)))
        LLRS(c, ::) := XX.t
      }

      LLRS := LLRS - z * math.log(z) + z * math.log(N)
      LLRS(LLRS.findAll(e => e.isNaN || e.isInfinity)) := 0.0
      val maxLLRS = breeze.linalg.max(LLRS(::, *))
      val critVal = DescriptiveStats.percentile(maxLLRS.t.data, prob)

      critVal
    }

  /** rmultinom(n, size, prob) from R
    *
    * @link https://stat.ethz.ch/R-manual/R-devel/library/stats/html/Multinom.html
    *      Here the implementation reference
    *      and here https://github.com/wch/r-source/blob/f8d4d7d48051860cc695b99db9be9cf439aee743/src/nmath/rmultinom.c
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
        val N = (size - breeze.linalg.sum(X(0 until j, i))).toInt

        val Binj = N match {
          case 0           => 0 // this can be simplified
          case n if n <= 0 => 0
          case _           => breeze.stats.distributions.Binomial(N, P).sample
        }

        X(j, i) = Binj.toDouble
      }
    }

    X
  }

}
