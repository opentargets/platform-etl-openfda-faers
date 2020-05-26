package io.opentargets.openfda.stages

import io.opentargets.openfda.SparkSessionSetup
import io.opentargets.openfda.stage.OpenFdaEtl
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OpenFdaEtlTest
    extends AnyWordSpecLike
    with Matchers
    with PrivateMethodTester
    with SparkSessionSetup {

  "The open fda ETL stage" should {
    "successfully load only drugs of interest" in withSparkSession { sparkSession =>
      val generateDrugList = PrivateMethod[Dataset[Row]]('generateDrugList)
      val drugList = OpenFdaEtl invokePrivate generateDrugList("chembl string", sparkSession)
      assert(drugList != null)
    }
  }

}
