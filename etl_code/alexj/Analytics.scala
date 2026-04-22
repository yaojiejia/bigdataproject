import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// ---- Metadata (mirrored in web/lib/figures.js) ----------------------------

case class MetricMeta(
    key: String,
    column: String,
    label: String,
    unit: String,
    higherIsBetter: Boolean
)

val METRICS: Seq[MetricMeta] = Seq(
  MetricMeta("score", "newcomer_score_100",  "Newcomer Score",             "/ 100",     higherIsBetter = true),
  MetricMeta("crime", "crimes_per_1k",       "Crimes per 1k residents",    "per 1k",    higherIsBetter = false),
  MetricMeta("food",  "critical_rate",       "Critical inspection rate",   "share",     higherIsBetter = false),
  MetricMeta("rent",  "median_rent_zori",    "Median rent (ZORI)",         "$ / month", higherIsBetter = false),
  MetricMeta("311",   "complaints_per_1k",   "311 complaints per 1k",      "per 1k",    higherIsBetter = false)
)

val INPUT_FEATURES: Seq[String] = Seq(
  "crimes_per_1k", "felony_share", "avg_score",
  "critical_rate", "complaints_per_1k", "median_rent_zori"
)

val RENT_PREDICTORS: Seq[String] = Seq(
  "crimes_per_1k", "felony_share", "avg_score",
  "critical_rate", "complaints_per_1k"
)

// 2-letter NTA prefix -> borough. Same mapping the dashboard uses.
val BOROUGH_FROM_CODE: Map[String, String] = Map(
  "MN" -> "Manhattan",
  "BK" -> "Brooklyn",
  "QN" -> "Queens",
  "BX" -> "Bronx",
  "SI" -> "Staten Island"
)

// ---- Paths ----------------------------------------------------------------

val dataRoot    = sys.env.getOrElse("DATA_ROOT", "./data")
val hdfsUser    = sys.env.getOrElse("HDFS_USER", "")
val prefix      = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser" else dataRoot
val inScore     = s"$prefix/scores/newcomer_score.parquet"
val outDir      = s"$prefix/analytics"

println(s"[analytics] reading $inScore")
println(s"[analytics] writing to $outDir")

spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

val src = spark.read.parquet(inScore)
val withBorough = src.withColumn(
  "borough",
  coalesce(
    element_at(
      typedLit(BOROUGH_FROM_CODE),
      substring(col("nta_code"), 1, 2)
    ),
    lit("Other")
  )
).cache()

val nRows = withBorough.count()
println(f"[analytics] input rows: $nRows%,d")

// ---- Hadoop FS helpers ----------------------------------------------------

val hadoopConf = spark.sparkContext.hadoopConfiguration
def fsFor(path: String): FileSystem = new HPath(path).getFileSystem(hadoopConf)

def writeSingleParquet(df: DataFrame, outFile: String): Unit = {
  val fs  = fsFor(outFile)
  val tmp = new HPath(outFile + ".tmp_dir")
  if (fs.exists(tmp)) fs.delete(tmp, true)
  df.coalesce(1).write.mode("overwrite").parquet(tmp.toString)
  val parts = fs.listStatus(tmp).filter { s =>
    val nm = s.getPath.getName
    nm.startsWith("part-") && nm.endsWith(".parquet")
  }
  require(parts.length == 1, s"expected one part file in $tmp, got ${parts.length}")
  val dst = new HPath(outFile)
  if (fs.exists(dst)) fs.delete(dst, true)
  fs.rename(parts(0).getPath, dst)
  fs.delete(tmp, true)
}

// Ensure the analytics dir exists.
{
  val fs = fsFor(outDir)
  val p  = new HPath(outDir)
  if (!fs.exists(p)) fs.mkdirs(p)
}

// ============================================================
// 1. summary.parquet
// ============================================================

def summaryRow(m: MetricMeta): Row = {
  val c   = col(m.column).cast("double")
  val s   = withBorough.filter(c.isNotNull).agg(
    avg(c).as("mean"),
    percentile_approx(c, lit(0.5), lit(1000)).as("median"),
    min(c).as("min"),
    max(c).as("max"),
    stddev_pop(c).as("std"),
    count(lit(1)).as("n_ntas")
  ).first()
  def nd(i: Int): java.lang.Double =
    Option(s.get(i)).map(v => java.lang.Double.valueOf(v.toString.toDouble)).orNull
  Row(
    m.key, m.column, m.label, m.unit, m.higherIsBetter,
    nd(0), nd(1), nd(2), nd(3), nd(4),
    s.getLong(5)
  )
}

val summarySchema = StructType(Seq(
  StructField("metric",           StringType,  nullable = false),
  StructField("column",           StringType,  nullable = false),
  StructField("label",            StringType,  nullable = false),
  StructField("unit",             StringType,  nullable = false),
  StructField("higher_is_better", BooleanType, nullable = false),
  StructField("mean",             DoubleType,  nullable = true),
  StructField("median",           DoubleType,  nullable = true),
  StructField("min",              DoubleType,  nullable = true),
  StructField("max",              DoubleType,  nullable = true),
  StructField("std",              DoubleType,  nullable = true),
  StructField("n_ntas",           LongType,    nullable = false)
))

val summaryDf = spark.createDataFrame(
  spark.sparkContext.parallelize(METRICS.map(summaryRow)),
  summarySchema
)
writeSingleParquet(summaryDf, s"$outDir/summary.parquet")
println(s"[analytics] wrote summary.parquet (${METRICS.size} metrics)")

// ============================================================
// 2. distribution.parquet — 24 equal-width bins per metric
// ============================================================
//
// We emit the pre-computed bin edges + counts so the browser doesn't have
// to crunch anything. The same cadence (24 bins) matches what the previous
// Python pipeline used with `go.Histogram(nbinsx=24)`.

val N_BINS = 24

// Iteration note: earlier versions of this block computed bin edges in the
// browser after loading the raw per-NTA values. That meant every dashboard
// load reparsed ~260 rows and recomputed min/max/width in JS — fine in
// isolation, but it forced the parquet reader (hyparquet) to ship the raw
// series over the wire per metric, and it duplicated binning logic between
// here and web/lib/figures.js. Moving binning server-side (here) means the
// dashboard fetches 5 metrics x 24 rows = 120 rows total and draws bars
// directly.
//
//   OLD (binned in the browser):
//     writeSingleParquet(
//       withBorough.select(METRICS.map(m => col(m.column)): _*),
//       s"$outDir/metrics_raw.parquet"
//     )
//     // ... then in figures.js:
//     //   const lo = Math.min(...values); const hi = Math.max(...values);
//     //   const width = (hi - lo) / 24;
//     //   const counts = new Array(24).fill(0);
//     //   values.forEach(v => counts[Math.min(23, Math.floor((v - lo) / width))]++);
def distributionRows(m: MetricMeta): DataFrame = {
  val c = col(m.column).cast("double")
  val filt = withBorough.filter(c.isNotNull)
  val mmr = filt.agg(min(c).as("lo"), max(c).as("hi")).first()
  val lo = mmr.getAs[Double]("lo")
  val hi = mmr.getAs[Double]("hi")
  if (hi == lo) {
    import spark.implicits._
    Seq((m.key, lo, lo, hi, filt.count())).toDF("metric", "bin_center", "bin_lo", "bin_hi", "count")
  } else {
    val width = (hi - lo) / N_BINS
    // Floor-based bucket index, clamped to N_BINS - 1 so the top edge lands in the last bin.
    val idxCol = least(lit(N_BINS - 1), floor((c - lit(lo)) / lit(width)))
    val binned = filt
      .withColumn("bin_index", idxCol)
      .groupBy("bin_index")
      .agg(count(lit(1)).as("count"))
    binned
      .withColumn("bin_lo",     lit(lo) + col("bin_index") * lit(width))
      .withColumn("bin_hi",     lit(lo) + (col("bin_index") + lit(1)) * lit(width))
      .withColumn("bin_center", (col("bin_lo") + col("bin_hi")) / lit(2))
      .withColumn("metric",     lit(m.key))
      .select("metric", "bin_center", "bin_lo", "bin_hi", "count")
      .orderBy("bin_center")
  }
}

val distributionDf = METRICS
  .map(distributionRows)
  .reduce(_ unionByName _)
writeSingleParquet(distributionDf, s"$outDir/distribution.parquet")
println(s"[analytics] wrote distribution.parquet")

// ============================================================
// 3. top_bottom.parquet — top/bottom 10 per metric
// ============================================================

val TOP_N = 10

def topBottomRows(m: MetricMeta): DataFrame = {
  val c   = col(m.column).cast("double")
  val src = withBorough.filter(c.isNotNull).select(col("nta_code"), col("nta_name"), c.as("value"))
  // Rank direction: "best" = high for higher-is-better, low otherwise.
  val descForBest = m.higherIsBetter
  val bestOrder  = if (descForBest) col("value").desc else col("value").asc
  val worstOrder = if (descForBest) col("value").asc  else col("value").desc
  val wBest      = Window.partitionBy().orderBy(bestOrder)
  val wWorst     = Window.partitionBy().orderBy(worstOrder)
  val best = src
    .withColumn("rank", row_number().over(wBest))
    .filter(col("rank") <= TOP_N)
    .withColumn("kind", lit("top"))
  val worst = src
    .withColumn("rank", row_number().over(wWorst))
    .filter(col("rank") <= TOP_N)
    .withColumn("kind", lit("bottom"))
  best.unionByName(worst)
    .withColumn("metric", lit(m.key))
    .select("metric", "kind", "rank", "nta_code", "nta_name", "value")
}

val topBottomDf = METRICS.map(topBottomRows).reduce(_ unionByName _)
writeSingleParquet(topBottomDf, s"$outDir/top_bottom.parquet")
println(s"[analytics] wrote top_bottom.parquet")

// ============================================================
// 4. borough_box.parquet
// ============================================================

def boroughBox(m: MetricMeta): DataFrame = {
  val c   = col(m.column).cast("double")
  val src = withBorough.filter(c.isNotNull)
  val quartiles = src
    .groupBy("borough")
    .agg(
      percentile_approx(c, lit(0.25), lit(1000)).as("q1"),
      percentile_approx(c, lit(0.50), lit(1000)).as("median"),
      percentile_approx(c, lit(0.75), lit(1000)).as("q3"),
      min(c).as("data_min"),
      max(c).as("data_max")
    )
    .withColumn("iqr", col("q3") - col("q1"))
    .withColumn("fence_lo", col("q1") - lit(1.5) * col("iqr"))
    .withColumn("fence_hi", col("q3") + lit(1.5) * col("iqr"))
    // Clip fences to the observed data range so Plotly doesn't draw whiskers
    // past the actual min/max. This matches Plotly's default box-plot rule.
    .withColumn("lowerfence", greatest(col("data_min"), col("fence_lo")))
    .withColumn("upperfence", least(col("data_max"),   col("fence_hi")))
    .withColumn("metric", lit(m.key))
    .select("metric", "borough", "q1", "median", "q3", "lowerfence", "upperfence")
  quartiles
}

val boroughBoxDf = METRICS.map(boroughBox).reduce(_ unionByName _)
writeSingleParquet(boroughBoxDf, s"$outDir/borough_box.parquet")
println(s"[analytics] wrote borough_box.parquet")

// ============================================================
// 5. borough_points.parquet — long-form (metric, borough, nta, value)
// ============================================================

def boroughPoints(m: MetricMeta): DataFrame = {
  val c = col(m.column).cast("double")
  withBorough.filter(c.isNotNull)
    .select(
      lit(m.key).as("metric"),
      col("borough"),
      col("nta_name"),
      c.as("value")
    )
}
val boroughPointsDf = METRICS.map(boroughPoints).reduce(_ unionByName _)
writeSingleParquet(boroughPointsDf, s"$outDir/borough_points.parquet")
println(s"[analytics] wrote borough_points.parquet")

// ============================================================
// 6. correlation.parquet — 6x6 Pearson on input features
// ============================================================

val corrRows: Seq[Row] = for {
  a <- INPUT_FEATURES
  b <- INPUT_FEATURES
} yield {
  val r =
    if (a == b) 1.0
    else {
      val v = withBorough
        .select(corr(col(a).cast("double"), col(b).cast("double")).as("r"))
        .first().get(0)
      if (v == null) Double.NaN else v.asInstanceOf[Number].doubleValue()
    }
  Row(a, b, r)
}
val corrSchema = StructType(Seq(
  StructField("feature_row", StringType, nullable = false),
  StructField("feature_col", StringType, nullable = false),
  StructField("r",           DoubleType, nullable = true)
))
val correlationDf = spark.createDataFrame(
  spark.sparkContext.parallelize(corrRows),
  corrSchema
)
writeSingleParquet(correlationDf, s"$outDir/correlation.parquet")
println(s"[analytics] wrote correlation.parquet")

// ============================================================
// 7. rent_vs_feature_{bins,ols,points}.parquet
// ============================================================
//
// Per predictor we emit three tables so the JS side is pure display:
//   - 10 quantile bins of the predictor with median + Q1/Q3 of rent
//   - one row of OLS coefficients + goodness-of-fit stats
//   - the raw (predictor, rent) points for the borough-coloured scatter
//
// Bin assignment uses ntile() which is exactly the "quantile with ties
// broken by row order" semantics pandas.qcut uses with duplicates='drop'.

val N_RENT_BINS = 10
val rentCol     = col("median_rent_zori").cast("double")

def rentVsBins(feat: String): DataFrame = {
  val x = col(feat).cast("double")
  val base = withBorough.filter(x.isNotNull && rentCol.isNotNull)
  val w = Window.orderBy(x)
  val binned = base.withColumn("bin_index", ntile(N_RENT_BINS).over(w))
  binned
    .groupBy("bin_index")
    .agg(
      percentile_approx(x,       lit(0.50), lit(1000)).as("x_mid"),
      percentile_approx(rentCol, lit(0.50), lit(1000)).as("rent_median"),
      percentile_approx(rentCol, lit(0.25), lit(1000)).as("rent_q1"),
      percentile_approx(rentCol, lit(0.75), lit(1000)).as("rent_q3"),
      count(lit(1)).as("n_ntas")
    )
    .withColumn("feature", lit(feat))
    .select("feature", "bin_index", "x_mid", "rent_median", "rent_q1", "rent_q3", "n_ntas")
    .orderBy("x_mid")
}

def rentVsOls(feat: String): Row = {
  val x = col(feat).cast("double")
  val base = withBorough.filter(x.isNotNull && rentCol.isNotNull)
    .select(x.as("x"), rentCol.as("y"))
  val stats = base.agg(
    avg("x").as("mx"),
    avg("y").as("my"),
    stddev_pop("x").as("sx"),
    stddev_pop("y").as("sy"),
    corr("x", "y").as("r"),
    min("x").as("x_min"),
    max("x").as("x_max"),
    percentile_approx(col("x"), lit(0.5), lit(1000)).as("x_med")
  ).first()

  val mx = stats.getAs[Double]("mx")
  val my = stats.getAs[Double]("my")
  val sx = stats.getAs[Double]("sx")
  val sy = stats.getAs[Double]("sy")
  val r  = Option(stats.get(4)).map(_.asInstanceOf[Number].doubleValue()).getOrElse(Double.NaN)
  val slope     = if (sx == 0.0) 0.0 else r * sy / sx
  val intercept = my - slope * mx
  val r2        = r * r
  val xMin  = stats.getAs[Double]("x_min")
  val xMax  = stats.getAs[Double]("x_max")
  val xMed  = stats.getAs[Double]("x_med")
  val yPred = slope * xMed + intercept
  Row(feat, slope, intercept, r, r2, xMin, xMax, xMed, yPred)
}

val olsSchema = StructType(Seq(
  StructField("feature",              StringType, nullable = false),
  StructField("slope",                DoubleType, nullable = true),
  StructField("intercept",            DoubleType, nullable = true),
  StructField("pearson_r",            DoubleType, nullable = true),
  StructField("r2",                   DoubleType, nullable = true),
  StructField("x_min",                DoubleType, nullable = true),
  StructField("x_max",                DoubleType, nullable = true),
  StructField("x_example_median",     DoubleType, nullable = true),
  StructField("y_example_predicted",  DoubleType, nullable = true)
))

def rentVsPoints(feat: String): DataFrame = {
  val x = col(feat).cast("double")
  withBorough
    .filter(x.isNotNull && rentCol.isNotNull)
    .select(
      lit(feat).as("feature"),
      col("nta_name"),
      col("borough"),
      x.as("x"),
      rentCol.as("y")
    )
}

val rentBinsDf   = RENT_PREDICTORS.map(rentVsBins).reduce(_ unionByName _)
val rentOlsDf    = spark.createDataFrame(
  spark.sparkContext.parallelize(RENT_PREDICTORS.map(rentVsOls)),
  olsSchema
)
val rentPointsDf = RENT_PREDICTORS.map(rentVsPoints).reduce(_ unionByName _)

writeSingleParquet(rentBinsDf,   s"$outDir/rent_vs_feature_bins.parquet")
writeSingleParquet(rentOlsDf,    s"$outDir/rent_vs_feature_ols.parquet")
writeSingleParquet(rentPointsDf, s"$outDir/rent_vs_feature_points.parquet")
println(s"[analytics] wrote rent_vs_feature_{bins,ols,points}.parquet")

println("\n=== Analytics.scala complete ===")
System.exit(0)
