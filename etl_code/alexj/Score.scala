import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

// ---- Weights + feature directions -----------------------------------------

// Iteration note: the very first version of these weights was an even split
// across the four dimensions. That produced a ranking dominated by cleanliness
// (311 volume correlates strongly with foot traffic / density, so central
// neighborhoods looked artificially bad). We rebalanced toward safety + rent
// after eyeballing the correlation heatmap in Analytics.scala.
//
//   OLD (v1, uniform):
//     val WEIGHTS: Map[String, Double] = Map(
//       "safety"        -> 0.25,
//       "food_safety"   -> 0.25,
//       "cleanliness"   -> 0.25,
//       "affordability" -> 0.25
//     )
//
//   OLD (v2, over-indexed on rent):
//     val WEIGHTS: Map[String, Double] = Map(
//       "safety"        -> 0.25,
//       "food_safety"   -> 0.20,
//       "cleanliness"   -> 0.10,
//       "affordability" -> 0.45
//     )
//   Dropped because affordability alone pushed UES / Tribeca to the bottom
//   even when every other signal was strong, which wasn't the story we
//   wanted to tell a newcomer.
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

// Iteration note: the first port from score.py used `stddev` here, which is
// Spark's sample std (ddof=1). pandas's .std() in the old pipeline used
// ddof=0 (population). That mismatch shifted every z-score by ~0.2% and
// changed the top/bottom 10 ordering for three NTAs that sat right on the
// tie boundary. Switched to `stddev_pop` so the Scala port reproduces the
// earlier output bit-for-bit (caught by diffing newcomer_score.parquet
// against the last known-good run).
//
//   OLD (off-by-ddof):
//     val statsAggs = FEATURE_DIRECTION.flatMap { case (c, _) =>
//       Seq(avg(col(c)).alias(s"mu_$c"), stddev(col(c)).alias(s"sd_$c"))
//     }
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

// ---- Suppress the score for data-starved NTAs -----------------------------
//
// Some NTAs are effectively non-residential (e.g. "The Battery-Governors
// Island-Ellis Island-Liberty Island" — four uninhabited islands grouped
// into one NTA) and carry almost no rent or inspection signal. The z-score
// stage above fills missing values with the column mean, which makes these
// NTAs look "average" on rent and food-safety. Those two dimensions together
// carry 0.55 of the weight, so the composite drifts — and in the actual data
// it's worse than drifting: Battery/Governors/Ellis/Liberty had 4 inspections
// with an unrealistically low avg_score of 1.25, pushing its raw newcomer
// score to the max in the city. If we rescaled before gating, that inflated
// max would squash everybody else's 0-100 number. So the order here is:
//   1. Decide who is data-starved.
//   2. Null their `newcomer_score`.
//   3. Only THEN compute min/max and rescale to 0-100 over the survivors.
//
// Gate: no ZORI rent AND fewer than MIN_INSPECTIONS restaurant inspections.
// The raw features stay in the parquet so the detail panel can still show
// whatever partial signal we have (crime counts, 311 counts). One-or-the-
// other missing is still enough to score the NTA.

val MIN_INSPECTIONS = 20L
val insufficient =
  col("median_rent_zori").isNull &&
  (col("n_inspections").isNull || col("n_inspections") < lit(MIN_INSPECTIONS))

val gated = subscored
  .withColumn("score_available", !insufficient)
  .withColumn(
    "newcomer_score",
    when(col("score_available"), col("newcomer_score")).otherwise(lit(null).cast("double"))
  )

val nUnscored = gated.filter(!col("score_available")).count()
if (nUnscored > 0) {
  println(s"\n[score] $nUnscored NTA(s) marked unscored (no ZORI rent + < $MIN_INSPECTIONS inspections):")
  gated
    .filter(!col("score_available"))
    .select("nta_code", "nta_name", "n_inspections", "median_rent_zori")
    .show(20, false)
}

// Min/max rescale over the surviving NTAs only. `min`/`max` ignore nulls.
val minmax = gated.agg(
  min("newcomer_score").as("lo"),
  max("newcomer_score").as("hi")
).first()
val lo = minmax.getAs[Double]("lo")
val hi = minmax.getAs[Double]("hi")

val rescaled =
  if (hi == lo) gated.withColumn("newcomer_score_100", lit(50.0))
  else gated.withColumn(
    "newcomer_score_100",
    when(col("newcomer_score").isNotNull,
      lit(100) * (col("newcomer_score") - lit(lo)) / lit(hi - lo)
    ).otherwise(lit(null).cast("double"))
  )

rescaled.cache()

// ---- Pretty-print top/bottom diagnostics (same as score.py) --------------

println("\nTop 10 NTAs by newcomer_score:")
rescaled
  .filter(col("newcomer_score").isNotNull)
  .orderBy(col("newcomer_score").desc)
  .select("nta_code", "nta_name", "newcomer_score", "newcomer_score_100")
  .limit(10).show(false)

println("Bottom 10 NTAs by newcomer_score:")
rescaled
  .filter(col("newcomer_score").isNotNull)
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
