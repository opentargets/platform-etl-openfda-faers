package io.opentargets.openfda.utils

import io.opentargets.openfda.config._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class ConfigurationTest extends AnyFlatSpecLike with Matchers {
  "Pureconfig" should "successfully load standard configuration" in {
    val conf = Configuration.config
    assert(conf != null)
  }
}

class FdaConfigurationTest extends AnyFlatSpecLike with Matchers {

  "Fda config" should "not accept invalid output formats" in {
    a[IllegalArgumentException] should be thrownBy {
      Fda(MonteCarlo(1, .04), FdaInputs(".txt", "json", "jsonl"), Seq("csv", "txt"), Sampling(""))
    }
  }

  "Fda config" should "accept output formats" in {
    val fdaConfig =
      Fda(MonteCarlo(1, .04),
          FdaInputs(".txt", "json", "jsonl"),
          Seq("csv", "json", "jsonl"),
          Sampling(""))
    assert(fdaConfig.outputs.length.equals(3))
  }

}

class MonteCarloConfigTest extends TableDrivenPropertyChecks with Matchers with AnyFlatSpecLike {
  private val monteCarloInvalidCombos =
    Table(
      ("permutations", "percentile"),
      (1, -0.4),
      (1, 1.1),
      (1, 0.0),
      (-1, 0.5),
      (0, 0.5)
    )
  "Fda config" should "not accept invalid output formats" in {
    forAll(monteCarloInvalidCombos) { (permutations: Int, percentile: Double) =>
      a[IllegalArgumentException] should be thrownBy {
        MonteCarlo(permutations, percentile)
      }
    }
  }

}

class FdaInputsTest extends AnyFlatSpecLike with TableDrivenPropertyChecks with Matchers {
  private val fdaInputInvalidCombos =
    Table(
      ("blist", "chembl", "fda"),
      ("txt", "json", "json"),
      ("txt", "json", "csv"),
      ("txt", "json", "html"),
      ("txt", "csv", "jsonl"),
      ("txt", "txt", "jsonl"),
      ("txt", "md", "jsonl"),
      ("csv", "json", "jsonl"),
      ("json", "json", "jsonl"),
      ("html", "json", "jsonl")
    )
  "Fda config" should "not accept invalid output formats" in {
    forAll(fdaInputInvalidCombos) { (blist: String, chembl: String, fda: String) =>
      a[IllegalArgumentException] should be thrownBy {
        FdaInputs(blist, chembl, fda)
      }
    }
  }
}
