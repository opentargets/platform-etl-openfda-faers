import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import better.files.Dsl._
import better.files._
import better.files._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vectors}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.stats.distributions.RandBasis

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

  val udfProbVector = udf((permutations: Int, n_j: Int, n_i: Seq[Long], n: Int, prob: Double) => {
    import breeze.linalg._
    import breeze.stats._
    // get the Pvector normalised by max element not sure i need to do it
    val z = n_j.toDouble
    val N = n.toDouble
    val y = convert(BDV(n_i.toArray), Double)
    val Pvector = y / N
    val maxPv = breeze.linalg.max(Pvector)
    Pvector := Pvector / maxPv

    println(s"values nj $n_j ni size ${n_i.length} n $n")
    // generate the multinorm permutations
    val mult = breeze.stats.distributions.Multinomial(Pvector)
    val x = BDM.zeros[Double](Pvector.size, permutations)

    // generate n_i columns
    for (i <- 0 until permutations) {
      x(::, i) := convert(BDV(mult.samples.take(Pvector.size).toArray), Double)
    }

    println(x.toString)

    // compute all llrs in one go
    val logx: BDM[Double] = breeze.numerics.log(x)
    val zx: BDM[Double] = z - x
    val logzx: BDM[Double] = breeze.numerics.log(zx)
    val logNy: BDV[Double] = breeze.numerics.log(N - y)
    val logy: BDV[Double] = breeze.numerics.log(y)

    // logLR <- x * (log(x) - log(y)) + (z-x) * (log(z - x) - log(n - y))
    // myLLRs <- myLLRs - n_j * log(n_j) + n_j * log(n)
    val logxy = logx(::,*) -:- logy
    val logzxy = logzx(::,*) -:- logNy
    val logLR = (x *:* logxy +:+ zx *:* logzxy) - z * math.log(z) + z * math.log(N)

    logLR(logLR.findAll(e => e.isNaN || e.isInfinity)) := 0.0

    val llrs = breeze.linalg.max(logLR(*,::))

    println(s"LLR ${llrs.toString}")

    // get the prob percentile value as a critical value
    val critVal = DescriptiveStats.percentile(llrs.data, prob)
    println(s"FDA CRITVAL ${critVal.toString}")

    critVal
  })

  val critVal = fdas
    .where($"chembl_id" === "CHEMBL714")
    .withColumn("n", $"D" + $"uniq_report_ids_by_drug" + $"uniq_report_ids_by_reaction" - $"A")
    .groupBy($"chembl_id")
    .agg(first($"uniq_report_ids_by_drug").as("n_j"),
      collect_list($"uniq_report_ids_by_reaction").as("n_i"),
      first($"n").as("n"))
    .withColumn("critVal", udfProbVector(lit(1000), $"n_j", $"n_i", $"n", lit(0.95)))

  fdas.join(critVal.select("chembl_id", "critVal"), Seq("chembl_id"), "inner")
    .write
    .json(outputPathPrefix + "/agg_critval/")
}
