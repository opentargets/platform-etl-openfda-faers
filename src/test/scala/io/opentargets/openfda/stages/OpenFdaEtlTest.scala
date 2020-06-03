package io.opentargets.openfda.stages

import io.opentargets.openfda.stage.OpenFdaEtl
import io.opentargets.openfda.utils.{Loaders, SparkSessionSetup}
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
    // private functions for tests
    val generateDrugList = PrivateMethod[Dataset[Row]]('generateDrugList)
    val filterBlacklist = PrivateMethod[Dataset[Row]]('filterBlacklist)

    "successfully load only drugs of interest" in withSparkSession { sparkSession =>
      // given

      // when
      val drugList = OpenFdaEtl invokePrivate generateDrugList(
        this.getClass.getResource("/drug_data500.json").getPath,
        sparkSession)
      // then
      val cols = drugList.columns
      val expectedColumns = List("chembl_id", "drug_name")

      assert(cols.length == expectedColumns.length)
      assert(cols.forall(colName => expectedColumns.contains(colName)))

    }
    "properly remove blacklisted events" in withSparkSession { sparkSession =>
      // given

      val df =
        sparkSession.read.json(this.getClass.getResource("/adverseEventSample.jsonl").getPath)
      val blPath: String = this.getClass.getResource("/blacklisted_events.txt").getPath
      val blackList = Loaders.loadBlackList(blPath)(sparkSession)
      // when
      val filtered = OpenFdaEtl invokePrivate filterBlacklist(blPath, df, sparkSession)

      // then
      /* This works as an inverse function of the UDF, where if there were items not removed
       * they would later be added to the blacklist giving up a larger list than the original*/
      assert(
        blackList
          .join(filtered,
                filtered("reaction_reactionmeddrapt") === blackList("reactions"),
                "left_anti")
          .count == blackList.count)

    }
  }

}
