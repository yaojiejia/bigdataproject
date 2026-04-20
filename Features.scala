// ============================================================
// Features.scala — Aggregate enriched datasets into one row per NTA.
// Run with:  spark-shell --master local[*] -i Features.scala
// ============================================================
//
// Input  : data/enriched/{crime,restaurants,complaints311,rent} (parquet)
//          data/geo/nta_population.csv (optional)
// Output : data/scores/neighborhood_features.parquet
//
// The four feature tables are outer-joined on (nta_code, nta_name). When
// population is available, we compute crimes_per_1k and complaints_per_1k;
// otherwise we pass raw counts through under those names so score.py sees the
// same schema either way.
//
// Iteration note: structural nulls (e.g. an NTA with no restaurants at all)
// are filled with zeros before writing, so the z-score stage never has to
// branch on "missing" vs "truly zero". Rent nulls are intentionally left as
// null — that signal is real (no rent observations in that NTA).
// ============================================================

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

val dataRoot = sys.env.getOrElse("DATA_ROOT", "./data")

// Iteration note: geocode.py writes enriched parquet via pandas/pyarrow, which
// emits nanosecond-precision timestamps (INT64 TIMESTAMP(NANOS)). Spark 3.5
// rejects that type by default ("Illegal Parquet type: INT64 (TIMESTAMP(NANOS,
// false))"). We never use any timestamp column here — only counts and numeric
// aggregates — but the Parquet reader validates the whole file schema up front.
// Enabling the legacy nanos-as-long read path lets Spark fall back gracefully
// for those columns while we keep ignoring them in selects.
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

val enrichedPrefix = s"$dataRoot/enriched"
val enrCrime       = s"$enrichedPrefix/crime"
val enrRestaurants = s"$enrichedPrefix/restaurants"
val enrComplaints  = s"$enrichedPrefix/complaints311"
val enrRent        = s"$enrichedPrefix/rent"

val ntaPopulation  = s"$dataRoot/geo/nta_population.csv"
val outFeatures    = s"$dataRoot/scores/neighborhood_features.parquet"

println(s"[features] reading enriched data from $enrichedPrefix")

// ---- Per-dataset aggregations ---------------------------------------------

def crimeFeatures(): DataFrame = {
  val df = spark.read.parquet(enrCrime).filter(col("nta_code").isNotNull)
  df.groupBy("nta_code", "nta_name")
    .agg(
      count(lit(1)).alias("total_crimes"),
      sum("IS_FELONY").alias("felonies")
    )
    .withColumn(
      "felony_share",
      when(col("total_crimes") > 0, col("felonies") / col("total_crimes")).otherwise(0.0)
    )
}

def restaurantFeatures(): DataFrame = {
  val df = spark.read.parquet(enrRestaurants).filter(col("nta_code").isNotNull)
  df.groupBy("nta_code", "nta_name")
    .agg(
      count(lit(1)).alias("n_inspections"),
      avg("SCORE").alias("avg_score"),
      avg("IS_CRITICAL").alias("critical_rate")
    )
}

def complaintFeatures(): DataFrame = {
  val df = spark.read.parquet(enrComplaints).filter(col("nta_code").isNotNull)
  df.groupBy("nta_code", "nta_name")
    .agg(count(lit(1)).alias("n_complaints"))
}

def rentFeatures(): DataFrame = {
  val df = spark.read.parquet(enrRent).filter(col("nta_code").isNotNull)
  df.groupBy("nta_code", "nta_name")
    .agg(avg("ZORI").alias("median_rent_zori"))
}

def loadPopulation(): Option[DataFrame] = {
  val f = new java.io.File(ntaPopulation)
  if (!f.exists()) return None
  val df = spark.read.option("header", true).option("inferSchema", true).csv(ntaPopulation)
  if (df.rdd.isEmpty()) None
  else Some(df.select(col("nta_code"), col("population").cast("double").alias("population")))
}

// ---- Drive -----------------------------------------------------------------

val crime = crimeFeatures()
val rest  = restaurantFeatures()
val c311  = complaintFeatures()
val rent  = rentFeatures()

var joined: DataFrame =
  crime.join(rest, Seq("nta_code", "nta_name"), "outer")
       .join(c311, Seq("nta_code", "nta_name"), "outer")
       .join(rent, Seq("nta_code", "nta_name"), "outer")

loadPopulation() match {
  case Some(pop) =>
    joined = joined.join(pop, Seq("nta_code"), "left")
    joined = joined
      .withColumn("crimes_per_1k",
        when(col("population") > 0, col("total_crimes") / (col("population") / 1000.0))
          .otherwise(lit(null).cast("double")))
      .withColumn("complaints_per_1k",
        when(col("population") > 0, col("n_complaints") / (col("population") / 1000.0))
          .otherwise(lit(null).cast("double")))
  case None =>
    // Without population, use raw totals as the intensity signal. Keeps score.py
    // free of branching on whether population is present.
    joined = joined
      .withColumn("crimes_per_1k", col("total_crimes").cast("double"))
      .withColumn("complaints_per_1k", col("n_complaints").cast("double"))
}

// Fill structural nulls (NTA with no restaurants, etc.). Rent stays nullable.
val fill: Map[String, Any] = Map(
  "total_crimes"      -> 0L,
  "felonies"          -> 0L,
  "felony_share"      -> 0.0,
  "n_inspections"     -> 0L,
  "critical_rate"     -> 0.0,
  "n_complaints"      -> 0L,
  "crimes_per_1k"     -> 0.0,
  "complaints_per_1k" -> 0.0
)
joined = joined.na.fill(fill)

val n = joined.count()
println(f"[features] $n%,d NTAs with aggregated features")
joined.coalesce(1).write.mode("overwrite").parquet(outFeatures)
println(s"[features] wrote $outFeatures")

println("\n=== Features.scala complete ===")
System.exit(0)
