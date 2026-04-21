// ============================================================
// Score.scala — Compose per-NTA features into a 0-100 newcomer score.
// Run with:
//   spark-shell --master local[*] -i etl_code/alexj/Score.scala
// Or just: make score
// ============================================================
//
// Input  : data/scores/neighborhood_features.parquet (Features.scala)
// Output : data/scores/newcomer_score.parquet        (single file)
//
// Scoring recipe (direct port of the previous score.py, preserved below so
// the weights stay auditable):
//
//   Weights (sum = 1.0)
//     safety        0.30   -> crimes_per_1k, felony_share
//     food_safety   0.25   -> avg_score, critical_rate
//     cleanliness   0.15   -> complaints_per_1k
//     affordability 0.30   -> median_rent_zori
//
//   For every feature we z-score (pop std, fillna with the column mean),
//   multiply by the direction (-1 = higher-is-worse, +1 = higher-is-better),
//   compose the four sub-scores as simple averages, weighted-sum them into
//   newcomer_score, and min-max rescale to 0-100 for the dashboard legend.
//
// Why Scala/Spark: the table is tiny (~260 rows), but we want the whole
// pipeline to be reproducible on Dataproc with the same job-submission
// pattern as clean/geocode/features. Using Spark here means no pandas
// anywhere in the big-data surface — the rubric the professor set.
//
// Iteration note: Spark's `stddev` is sample-std (n-1); pandas used `ddof=0`
// (population). To match the earlier output bit-for-bit we use
// `stddev_pop` on a `groupBy().agg(...)` over a constant key.
// ============================================================

import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

// ---- Weights + feature directions -----------------------------------------

val WEIGHTS: Map[String, Double] = Map(
  "safety"        -> 0.30,
  "food_safety"   -> 0.25,
  "cleanliness"   -> 0.15,
  "affordability" -> 0.30
)

// +1 = higher is better, -1 = higher is worse. Kept identical to score.py.
val FEATURE_DIRECTION: Seq[(String, Int)] = Seq(
  "crimes_per_1k"     -> -1,
  "felony_share"      -> -1,
  "avg_score"         -> -1, // lower health inspection score = cleaner
  "critical_rate"     -> -1,
  "complaints_per_1k" -> -1,
  "median_rent_zori"  -> -1  // higher rent = worse affordability
)

// ---- Path resolution -------------------------------------------------------

val dataRoot    = sys.env.getOrElse("DATA_ROOT", "./data")
val hdfsUser    = sys.env.getOrElse("HDFS_USER", "")
val scoresPrefix = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/scores" else s"$dataRoot/scores"

val inFeatures = s"$scoresPrefix/neighborhood_features.parquet"
val outScore   = s"$scoresPrefix/newcomer_score.parquet"

println(s"[score] reading $inFeatures")
println(s"[score] writing $outScore")

spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

// ---- Load features --------------------------------------------------------

val features = spark.read.parquet(inFeatures).cache()
val n = features.count()
println(f"[score] $n%,d NTAs")

// ---- Compute per-column mean + pop-std in a single reduction -------------

val statsAggs = FEATURE_DIRECTION.flatMap { case (c, _) =>
  Seq(
    avg(col(c).cast("double")).alias(s"mu_$c"),
    stddev_pop(col(c).cast("double")).alias(s"sd_$c")
  )
}
val statsRow: Row = features.agg(statsAggs.head, statsAggs.tail: _*).first()
val stats: Map[String, (Double, Double)] = FEATURE_DIRECTION.map { case (c, _) =>
  val mu = Option(statsRow.getAs[Any](s"mu_$c")).map(_.asInstanceOf[Number].doubleValue()).getOrElse(0.0)
  val sd = Option(statsRow.getAs[Any](s"sd_$c")).map(_.asInstanceOf[Number].doubleValue()).getOrElse(0.0)
  c -> (mu, sd)
}.toMap

FEATURE_DIRECTION.foreach { case (c, _) =>
  val (mu, sd) = stats(c)
  println(f"[score] $c%-20s mu=$mu%12.4f  sd=$sd%12.4f")
}

// ---- Z-score with nulls -> mean, sd == 0 -> zero -------------------------

var scored: DataFrame = features
FEATURE_DIRECTION.foreach { case (c, direction) =>
  val (mu, sd) = stats(c)
  val base     = coalesce(col(c).cast("double"), lit(mu))
  val z        = if (sd == 0.0 || sd.isNaN) lit(0.0) else (base - lit(mu)) / lit(sd) * lit(direction.toDouble)
  scored = scored.withColumn(s"z_$c", z)
}

// Sub-scores -> weighted sum -> min/max rescale to 0-100.

val subscored = scored
  .withColumn("safety_score",        (col("z_crimes_per_1k") + col("z_felony_share")) / lit(2))
  .withColumn("food_safety_score",   (col("z_avg_score") + col("z_critical_rate")) / lit(2))
  .withColumn("cleanliness_score",   col("z_complaints_per_1k"))
  .withColumn("affordability_score", col("z_median_rent_zori"))
  .withColumn(
    "newcomer_score",
    lit(WEIGHTS("safety"))        * col("safety_score")
      + lit(WEIGHTS("food_safety"))   * col("food_safety_score")
      + lit(WEIGHTS("cleanliness"))   * col("cleanliness_score")
      + lit(WEIGHTS("affordability")) * col("affordability_score")
  )

val minmax = subscored.agg(
  min("newcomer_score").as("lo"),
  max("newcomer_score").as("hi")
).first()
val lo = minmax.getAs[Double]("lo")
val hi = minmax.getAs[Double]("hi")

val rescaled =
  if (hi == lo) subscored.withColumn("newcomer_score_100", lit(50.0))
  else subscored.withColumn(
    "newcomer_score_100",
    lit(100) * (col("newcomer_score") - lit(lo)) / lit(hi - lo)
  )

rescaled.cache()

// ---- Pretty-print top/bottom diagnostics (same as score.py) --------------

println("\nTop 10 NTAs by newcomer_score:")
rescaled
  .orderBy(col("newcomer_score").desc)
  .select("nta_code", "nta_name", "newcomer_score", "newcomer_score_100")
  .limit(10).show(false)

println("Bottom 10 NTAs by newcomer_score:")
rescaled
  .orderBy(col("newcomer_score").asc)
  .select("nta_code", "nta_name", "newcomer_score", "newcomer_score_100")
  .limit(10).show(false)

// ---- Write as a single parquet file so the static frontend can fetch
// it at a predictable URL. Spark writes a directory; we hoist the lone
// part file up to the intended filename and drop the temp dir.

def writeSingleParquet(df: DataFrame, outFile: String): Unit = {
  val conf  = spark.sparkContext.hadoopConfiguration
  val hPath = new HPath(outFile)
  val fs    = hPath.getFileSystem(conf)
  val tmp   = new HPath(outFile + ".tmp_dir")
  if (fs.exists(tmp)) fs.delete(tmp, true)
  df.coalesce(1).write.mode("overwrite").parquet(tmp.toString)
  val parts = fs.listStatus(tmp).filter { s =>
    val nm = s.getPath.getName
    nm.startsWith("part-") && nm.endsWith(".parquet")
  }
  require(parts.length == 1, s"expected one part file in $tmp, got ${parts.length}")
  if (fs.exists(hPath)) fs.delete(hPath, true)
  fs.rename(parts(0).getPath, hPath)
  fs.delete(tmp, true)
}

writeSingleParquet(rescaled, outScore)
println(s"[score] wrote $outScore (${rescaled.count()} rows)")

println("\n=== Score.scala complete ===")
System.exit(0)
