package io.opentargets.openfda

import io.opentargets.openfda.utils.Loaders
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class LoadersTest extends AnyWordSpecLike with Matchers with SparkSessionSetup {

  "Loaders" should {
    "import the blacklist from resources" in withSparkSession { sparkSession =>
      val blacklist =
        Loaders.loadBlackList(this.getClass.getResource("/blacklisted_events.txt").getPath)(
          sparkSession)
      assert(!blacklist.isEmpty)
      assert(blacklist.columns.length == 1)
    }
  }
}

trait SparkSessionSetup {
  def withSparkSession(testMethod: SparkSession => Any) {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("testSparkTrait")
      .config("spark.driver.maxResultSize", "0")
      .getOrCreate()
    try {
      testMethod(spark)
    } finally spark.stop()
  }
}
