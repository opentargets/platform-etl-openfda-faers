package io.opentargets.openfda.utils

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MathUtilsTest extends AnyFlatSpecLike with Matchers {
  import breeze.linalg._

  "Each row of rmultinomial probabilities" should "sum to 'size' where 'size' is the number of options" in {
    val size = 10
    val multis: BDM[Double] =
      MathUtils.rmultinom(10, size, BDV.rand(size))
    val rowSums = sum(multis, Axis._0)
    assert(rowSums.inner.forall(s => s == size))
  }

  "Each column" should "represent a random sample from the distribution" in {
    val size, iters = 10
    val multis: BDM[Double] =
      MathUtils.rmultinom(iters, size, BDV.rand(size))
    val colMax = max(multis, Axis._1)
    val colMin = min(multis, Axis._1)
    val diffs = colMax - colMin
    /*
    Each row is a random sample, and each column the allocation for bucket n. The column
    value should change across samples. By summing the diffs we know that there must have
    been different allocations across samples.
     */
    sum(diffs) should not be 0
  }

  "rmultinomial probabilities" should "be 1 when there is only 1 option" in {
    val multis = MathUtils.rmultinom(1, 1, BDV(Array(1D)))
    assert(multis.data(0) == 1D)
  }
}
