// ============================================================
// Clean.scala — Full batch cleaner for all four datasets.
// Run with:
//   spark-shell --master local[*] -i Clean.scala
// or on Dataproc:
//   HDFS_USER=you spark-shell --master yarn --deploy-mode client -i Clean.scala
// ============================================================
//
// Reads four raw inputs from $DATA_ROOT/raw (local) or hdfs:///user/$HDFS_USER/data
// (cluster), and writes cleaned Parquet to data/cleaned/{crime,restaurants,
// complaints311,rent}.
//
// Schemas below are fixed contracts — they are what pipeline/geocode.py and
// Features.scala expect. If you change a schema here, update both consumers.
//
// Iteration note: an earlier version of clean_restaurants dropped Latitude and
// Longitude in its projection. That silently broke geocoding downstream because
// every row failed the point-in-polygon join. Both coordinates are now kept and
// dropna'd the same way the other point datasets handle them. The broken
// projection is preserved as a comment inside the cleanRestaurants block below.
// ============================================================

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val CRITICAL_SCORE_THRESHOLD = 28

// ---- Path resolution (local-first, HDFS if HDFS_USER is set) ---------------

val dataRoot    = sys.env.getOrElse("DATA_ROOT", "./data")
val hdfsUser    = sys.env.getOrElse("HDFS_USER", "")
val rawPrefix   = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/data"    else s"$dataRoot/raw"
val cleanPrefix = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/cleaned" else s"$dataRoot/cleaned"

val rawCrime        = s"$rawPrefix/nypd_complaints.csv"
val rawRestaurants  = s"$rawPrefix/restaurant_inspections.csv"
val rawComplaints   = s"$rawPrefix/complaints_311_food.csv"
val rawRent         = s"$rawPrefix/zori_zip.csv"

val outCrime       = s"$cleanPrefix/crime"
val outRestaurants = s"$cleanPrefix/restaurants"
val outComplaints  = s"$cleanPrefix/complaints311"
val outRent        = s"$cleanPrefix/rent"

println(s"[clean] DATA_ROOT=$dataRoot  HDFS_USER=${if (hdfsUser.nonEmpty) hdfsUser else "(local)"}")
println(s"[clean] reading from $rawPrefix")
println(s"[clean] writing to   $cleanPrefix")

// ---- Helpers ---------------------------------------------------------------

def readCsv(path: String): DataFrame =
  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("escape", "\"")
    .csv(path)

def reportCount(name: String, before: Long, after: Long): Unit =
  println(f"[$name] $before%,d -> $after%,d")

// ============================================================
// 1. CRIME
// ============================================================
def cleanCrime(): DataFrame = {
  val df = readCsv(rawCrime)
  val before = df.count()
  val cleaned = df
    .select(
      col("CMPLNT_NUM"), col("CMPLNT_FR_DT"),
      col("OFNS_DESC"), col("LAW_CAT_CD"),
      col("BORO_NM"), col("Latitude"), col("Longitude")
    )
    .na.drop(Seq("BORO_NM", "OFNS_DESC", "Latitude", "Longitude"))
    .withColumn("BORO_NM",    upper(trim(col("BORO_NM"))))
    .withColumn("OFNS_DESC",  upper(trim(col("OFNS_DESC"))))
    .withColumn("LAW_CAT_CD", upper(trim(col("LAW_CAT_CD"))))
    .filter(col("BORO_NM") =!= "(NULL)" && col("BORO_NM") =!= "")
    .withColumn("IS_FELONY", when(col("LAW_CAT_CD") === "FELONY", 1).otherwise(0))
  reportCount("crime", before, cleaned.count())
  cleaned
}

// ============================================================
// 2. RESTAURANTS
// ============================================================
def cleanRestaurants(): DataFrame = {
  val df = readCsv(rawRestaurants)
  val before = df.count()

  // Iteration note: an earlier version of this projection mirrored Clean.scala
  // v1 and dropped Latitude/Longitude. Cleaning looked fine; geocode.py then
  // blew up with KeyError on the spatial join because no rows had coordinates.
  // Preserving the broken projection below as a warning:
  //
  //   OLD (broken):
  //     .select("CAMIS", "DBA", "BORO", "ZIPCODE",
  //             col("CUISINE DESCRIPTION").as("CUISINE"),
  //             col("INSPECTION DATE").as("INSPECTION_DATE"),
  //             "SCORE", "GRADE")
  //     .na.drop(Seq("BORO", "SCORE"))

  val cleaned = df
    .select(
      col("CAMIS"), col("DBA"), col("BORO"), col("ZIPCODE"),
      col("CUISINE DESCRIPTION").as("CUISINE"),
      col("INSPECTION DATE").as("INSPECTION_DATE"),
      col("SCORE"), col("GRADE"),
      col("Latitude"), col("Longitude")
    )
    .na.drop(Seq("BORO", "SCORE", "Latitude", "Longitude"))
    .withColumn("BORO",    upper(trim(col("BORO"))))
    .withColumn("DBA",     upper(trim(col("DBA"))))
    .withColumn("CUISINE", upper(trim(col("CUISINE"))))
    .withColumn("GRADE",   upper(trim(col("GRADE"))))
    .filter(col("BORO") =!= "0" && col("BORO") =!= "")
    .withColumn("IS_CRITICAL", when(col("SCORE") >= CRITICAL_SCORE_THRESHOLD, 1).otherwise(0))
  reportCount("restaurants", before, cleaned.count())
  cleaned
}

// ============================================================
// 3. 311 FOOD-SAFETY COMPLAINTS
// ============================================================
def clean311(): DataFrame = {
  val df = readCsv(rawComplaints)
  val before = df.count()
  val cleaned = df
    .select(
      col("unique_key").as("COMPLAINT_ID"),
      col("created_date").as("CREATED_DATE"),
      col("complaint_type").as("COMPLAINT_TYPE"),
      col("descriptor").as("DESCRIPTOR"),
      col("incident_zip").as("ZIPCODE"),
      col("borough").as("BORO"),
      col("latitude").as("Latitude"),
      col("longitude").as("Longitude")
    )
    .na.drop(Seq("COMPLAINT_TYPE", "Latitude", "Longitude"))
    .withColumn("BORO",           upper(trim(col("BORO"))))
    .withColumn("COMPLAINT_TYPE", upper(trim(col("COMPLAINT_TYPE"))))
    .filter(col("BORO") =!= "UNSPECIFIED" && col("BORO").isNotNull)
  reportCount("311 food", before, cleaned.count())
  cleaned
}

// ============================================================
// 4. ZILLOW ZORI RENT  (wide-to-long, pick latest month per ZIP)
// ============================================================
def cleanRent(): DataFrame = {
  val df = readCsv(rawRent)
  val allCols = df.columns.toSeq
  // Month columns start with 4 digits (YYYY-MM-DD format Zillow uses).
  val dateCols = allCols.filter(c => c.length >= 4 && c.substring(0, 4).forall(_.isDigit))
  val idCols   = allCols.filterNot(dateCols.contains)

  // Keep rows whose City is New York OR Metro mentions NYC.
  val hasMsa = idCols.contains("MsaName")
  val nycFilter =
    col("City") === "New York" ||
    col("Metro").rlike("(?i)new york") ||
    (if (hasMsa) col("MsaName").rlike("(?i)new york") else lit(false))
  val nyc = df.filter(nycFilter)

  // Melt to long: stack(n, 'month1', `month1`, 'month2', `month2`, ...) as (month, zori)
  val pairs = dateCols.map(c => s"'$c', `$c`").mkString(",")
  val stackExpr = s"stack(${dateCols.size}, $pairs) as (month, zori)"
  val keep = Seq("RegionName", "City", "State", "Metro").filter(idCols.contains)
  val long = nyc.selectExpr((keep :+ stackExpr): _*).filter(col("zori").isNotNull)

  // Latest non-null month per ZIP.
  val w = Window.partitionBy("RegionName").orderBy(col("month").desc)
  val latest = long
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") === 1)
    .drop("rn")
    .withColumnRenamed("RegionName", "ZIPCODE")
    .withColumnRenamed("zori", "ZORI")
    .select("ZIPCODE", "City", "State", "Metro", "month", "ZORI")
  println(f"[rent] ${latest.count()}%,d NYC ZIPs")
  latest
}

// ============================================================
// DRIVE
// ============================================================

println("\n=== Cleaning crime ===")
val crime = cleanCrime()
crime.write.mode("overwrite").parquet(outCrime)
println(s"  wrote $outCrime")

println("\n=== Cleaning restaurants ===")
val rest = cleanRestaurants()
rest.write.mode("overwrite").parquet(outRestaurants)
println(s"  wrote $outRestaurants")

println("\n=== Cleaning 311 ===")
val c311 = clean311()
c311.write.mode("overwrite").parquet(outComplaints)
println(s"  wrote $outComplaints")

println("\n=== Cleaning rent ===")
val rent = cleanRent()
rent.write.mode("overwrite").parquet(outRent)
println(s"  wrote $outRent")

println("\n=== Clean.scala complete ===")
System.exit(0)
