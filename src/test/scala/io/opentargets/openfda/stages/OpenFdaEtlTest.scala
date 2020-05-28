package io.opentargets.openfda.stages

import io.opentargets.openfda.stage.OpenFdaEtl
import io.opentargets.openfda.utils.SparkSessionSetup
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
      // given
      val generateDrugList = PrivateMethod[Dataset[Row]]('generateDrugList)
      // when
      val drugList = OpenFdaEtl invokePrivate generateDrugList(
        this.getClass.getResource("/drug_data500.jsonl").getPath,
        sparkSession)
      // then
      val cols = drugList.columns
      val expectedColumns = List("chembl_id", "drug_name", "target_id")

      assert(cols.length == expectedColumns.length)
      assert(cols.forall(colName => expectedColumns.contains(colName)))

    }
  }

}
