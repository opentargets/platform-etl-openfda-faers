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
      Fda(MonteCarlo(1, .04),
          FdaInputs(".txt", "json", "jsonl", Option("asc")),
          Seq("csv", "txt"),
          Sampling(""))
    }
  }

  "Fda config" should "accept output formats" in {
    val fdaConfig =
      Fda(MonteCarlo(1, .04),
          FdaInputs(".txt", "json", "jsonl", None),
          Seq("csv", "json", "parquet"),
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
      ("blist", "chembl", "fda", "meddra"),
      ("txt", "json", "json", Option("asc")),
      ("txt", "json", "csv", Option("asbc")),
      ("txt", "json", "html", Option("asc")),
      ("txt", "csv", "jsonl", Option("asc")),
      ("txt", "txt", "jsonl", Option("asc")),
      ("txt", "md", "jsonl", Option("asd")),
      ("csv", "json", "jsonl", Option("asc")),
      ("json", "json", "jsonl", Option("asc")),
      ("html", "json", "jsonl", Option("asc"))
    )
  "Fda config" should "not accept invalid output formats" in {
    forAll(fdaInputInvalidCombos) { (blist: String, chembl: String, fda: String, meddra) =>
      a[IllegalArgumentException] should be thrownBy {
        FdaInputs(blist, chembl, fda, meddra)
      }
    }
  }
}
