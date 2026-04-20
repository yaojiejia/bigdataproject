// ============================================================
// FirstCode.scala — Initial Code Analysis & Cleaning
// Run with:  spark-shell --master yarn --deploy-mode client
//            :load FirstCode.scala
// ============================================================
//
// DATASETS:
//   1. NYPD Complaint Data (Current YTD) — hdfs:///user/<you>/data/nypd_complaints.csv
//   2. DOHMH Restaurant Inspection Results — hdfs:///user/<you>/data/restaurant_inspections.csv
//
// THIS FILE PERFORMS:
//   Part A — Statistical analysis (mean, median, mode, std dev) on numerical columns
//   Part B — Data cleaning using TWO cleaning options:
//       CLEANING OPTION 1: Text formatting (trimming spaces, uppercasing borough names for normalization)
//       CLEANING OPTION 2: Binary column creation based on condition of another column
//   Part C — MapReduce-style aggregation using RDD map() and reduceByKey()
//
// OUTPUT: Cleaned and enriched DataFrames written as Parquet to HDFS
// ============================================================

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Local-first: reads from $DATA_ROOT (default ./data) unless HDFS_USER is set.
val dataRoot = sys.env.getOrElse("DATA_ROOT", "./data")
val hdfsUser = sys.env.getOrElse("HDFS_USER", "")
val rawPrefix   = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/data" else s"$dataRoot/raw"
val cleanPrefix = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/cleaned" else s"$dataRoot/cleaned"

// ============================================================
// LOAD DATASETS
// ============================================================
println("=== Loading Datasets ===")

val crime = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", "\"").csv(s"$rawPrefix/nypd_complaints.csv")

val restaurants = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", "\"").csv(s"$rawPrefix/restaurant_inspections.csv")

println(s"Crime records loaded:      ${crime.count()}")
println(s"Restaurant records loaded:  ${restaurants.count()}")

// ============================================================
// PART A: STATISTICAL ANALYSIS
// ============================================================

// --------------------------------------------------------------
// A1. Restaurant Inspection SCORE — Mean, Median, Mode, Std Dev
// --------------------------------------------------------------
println("\n=== Part A1: Restaurant Inspection SCORE Statistics ===")

// Filter to valid numeric scores only
val validScores = restaurants.filter(col("SCORE").isNotNull && col("SCORE") >= 0)

// MEAN
val scoreMean = validScores.agg(avg("SCORE")).first().getDouble(0)
println(s"MEAN of SCORE:              $scoreMean")

// MEDIAN (50th percentile)
val scoreMedian = validScores.stat.approxQuantile("SCORE", Array(0.5), 0.001)(0)
println(s"MEDIAN of SCORE:            $scoreMedian")

// MODE (most frequently occurring value)
val scoreMode = validScores.groupBy("SCORE").count().orderBy(desc("count")).first().get(0)
println(s"MODE of SCORE:              $scoreMode")

// STANDARD DEVIATION
val scoreStdDev = validScores.agg(stddev("SCORE")).first().getDouble(0)
println(s"STD DEVIATION of SCORE:     $scoreStdDev")

// Full summary statistics for SCORE
println("\n--- SCORE describe() ---")
validScores.describe("SCORE").show()

// --------------------------------------------------------------
// A2. Crime Data — Latitude/Longitude Statistics
// --------------------------------------------------------------
println("\n=== Part A2: Crime Data Latitude Statistics ===")

val validCrimeGeo = crime.filter(col("Latitude").isNotNull && col("Latitude") > 0)

// MEAN
val latMean = validCrimeGeo.agg(avg("Latitude")).first().getDouble(0)
println(s"MEAN of Latitude:           $latMean")

// MEDIAN
val latMedian = validCrimeGeo.stat.approxQuantile("Latitude", Array(0.5), 0.001)(0)
println(s"MEDIAN of Latitude:         $latMedian")

// MODE
val latMode = validCrimeGeo.groupBy("Latitude").count().orderBy(desc("count")).first().get(0)
println(s"MODE of Latitude:           $latMode")

// STANDARD DEVIATION
val latStdDev = validCrimeGeo.agg(stddev("Latitude")).first().getDouble(0)
println(s"STD DEVIATION of Latitude:  $latStdDev")

println("\n--- Latitude describe() ---")
validCrimeGeo.describe("Latitude").show()

// --------------------------------------------------------------
// A3. Crime Data — Longitude Statistics
// --------------------------------------------------------------
println("\n=== Part A3: Crime Data Longitude Statistics ===")

val lonMean = validCrimeGeo.agg(avg("Longitude")).first().getDouble(0)
println(s"MEAN of Longitude:          $lonMean")

val lonMedian = validCrimeGeo.stat.approxQuantile("Longitude", Array(0.5), 0.001)(0)
println(s"MEDIAN of Longitude:        $lonMedian")

val lonStdDev = validCrimeGeo.agg(stddev("Longitude")).first().getDouble(0)
println(s"STD DEVIATION of Longitude: $lonStdDev")

println("\n--- Longitude describe() ---")
validCrimeGeo.describe("Longitude").show()

// ============================================================
// PART B: DATA CLEANING
// ============================================================

// --------------------------------------------------------------
// CLEANING OPTION 1: TEXT FORMATTING
// Trim whitespace, remove extra characters, uppercase borough
// names for normalization and future joining across datasets.
// This ensures "Manhattan", " MANHATTAN", "manhattan " all
// become "MANHATTAN" so the two datasets can be joined on borough.
// --------------------------------------------------------------
println("\n=== Part B — Cleaning Option 1: Text Formatting ===")

// Crime: trim and uppercase BORO_NM, trim OFNS_DESC
val crimeCleaned = crime.select("CMPLNT_NUM", "CMPLNT_FR_DT", "OFNS_DESC", "LAW_CAT_CD", "BORO_NM", "Latitude", "Longitude").na.drop(Seq("BORO_NM", "OFNS_DESC", "Latitude", "Longitude")).withColumn("BORO_NM", upper(trim(col("BORO_NM")))).withColumn("OFNS_DESC", upper(trim(col("OFNS_DESC")))).withColumn("LAW_CAT_CD", upper(trim(col("LAW_CAT_CD")))).filter(col("BORO_NM") =!= "(NULL)" && col("BORO_NM") =!= "")

println("Crime data — text formatting applied (upper + trim on BORO_NM, OFNS_DESC, LAW_CAT_CD)")
println(s"Crime records after text cleaning: ${crimeCleaned.count()}")
crimeCleaned.show(5, false)

// Restaurants: trim and uppercase BORO, trim DBA and CUISINE DESCRIPTION
val restCleaned = restaurants.select("CAMIS", "DBA", "BORO", "ZIPCODE", "CUISINE DESCRIPTION", "INSPECTION DATE", "SCORE", "GRADE").na.drop(Seq("BORO", "SCORE")).withColumn("BORO", upper(trim(col("BORO")))).withColumn("DBA", upper(trim(col("DBA")))).withColumn("CUISINE DESCRIPTION", upper(trim(col("CUISINE DESCRIPTION")))).withColumn("GRADE", upper(trim(col("GRADE")))).filter(col("BORO") =!= "0" && col("BORO") =!= "")

println("\nRestaurant data — text formatting applied (upper + trim on BORO, DBA, CUISINE DESCRIPTION, GRADE)")
println(s"Restaurant records after text cleaning: ${restCleaned.count()}")
restCleaned.show(5, false)

// --------------------------------------------------------------
// CLEANING OPTION 2: BINARY COLUMN BASED ON CONDITION
// Crime: IS_FELONY — 1 if LAW_CAT_CD is "FELONY", 0 otherwise.
//   This allows quick filtering and aggregation of serious crimes.
// Restaurants: IS_CRITICAL — 1 if SCORE >= 28 (grade C threshold), 0 otherwise.
//   Scores >= 28 indicate critical health violations.
// --------------------------------------------------------------
println("\n=== Part B — Cleaning Option 2: Binary Column Creation ===")

// Crime: binary column IS_FELONY (1 if felony, 0 otherwise)
val crimeWithBinary = crimeCleaned.withColumn("IS_FELONY", when(col("LAW_CAT_CD") === "FELONY", 1).otherwise(0))

println("Crime data — added IS_FELONY column (1 = FELONY, 0 = MISDEMEANOR/VIOLATION)")
crimeWithBinary.select("CMPLNT_NUM", "OFNS_DESC", "LAW_CAT_CD", "BORO_NM", "IS_FELONY").show(10, false)

// Show distribution of felony vs non-felony
println("IS_FELONY distribution:")
crimeWithBinary.groupBy("IS_FELONY").count().show()

// Restaurants: binary column IS_CRITICAL (1 if score >= 28, 0 otherwise)
val restWithBinary = restCleaned.withColumn("IS_CRITICAL", when(col("SCORE") >= 28, 1).otherwise(0))

println("Restaurant data — added IS_CRITICAL column (1 = SCORE >= 28 critical, 0 = acceptable)")
restWithBinary.select("CAMIS", "DBA", "SCORE", "GRADE", "IS_CRITICAL").show(10, false)

// Show distribution
println("IS_CRITICAL distribution:")
restWithBinary.groupBy("IS_CRITICAL").count().show()

// ============================================================
// PART C: MAPREDUCE-STYLE AGGREGATION
// Uses RDD map() and reduceByKey() to count records per borough
// This demonstrates classic MapReduce pattern in Spark
// ============================================================
println("\n=== Part C: MapReduce — Crime Counts per Borough ===")

// MAP: each crime record -> (borough, 1)
// REDUCE: sum counts per borough
val crimeByBorough = crimeWithBinary.rdd.map(row => (row.getAs[String]("BORO_NM"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2)
crimeByBorough.foreach { case (boro, count) => println(s"  $boro: $count") }

println("\n=== Part C: MapReduce — Felonies per Borough ===")

// MAP: filter to felonies, then (borough, 1)
// REDUCE: sum
val felonyByBorough = crimeWithBinary.filter(col("IS_FELONY") === 1).rdd.map(row => (row.getAs[String]("BORO_NM"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2)
felonyByBorough.foreach { case (boro, count) => println(s"  $boro: $count") }

println("\n=== Part C: MapReduce — Restaurant Inspections per Borough ===")

// MAP: each restaurant record -> (borough, 1)
// REDUCE: sum counts per borough
val restByBorough = restWithBinary.rdd.map(row => (row.getAs[String]("BORO"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2)
restByBorough.foreach { case (boro, count) => println(s"  $boro: $count") }

println("\n=== Part C: MapReduce — Critical Violations per Borough ===")

// MAP: filter to critical, then (borough, 1)
// REDUCE: sum
val critByBorough = restWithBinary.filter(col("IS_CRITICAL") === 1).rdd.map(row => (row.getAs[String]("BORO"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2)
critByBorough.foreach { case (boro, count) => println(s"  $boro: $count") }

// ============================================================
// WRITE FINAL CLEANED + ENRICHED DATA TO HDFS
// ============================================================
println("\n=== Writing Final Data to HDFS ===")

crimeWithBinary.write.mode("overwrite").parquet(s"$cleanPrefix/crime")
println(s"Wrote crime data (with IS_FELONY) to $cleanPrefix/crime")

restWithBinary.write.mode("overwrite").parquet(s"$cleanPrefix/restaurants")
println(s"Wrote restaurant data (with IS_CRITICAL) to $cleanPrefix/restaurants")

// Verify
println("\n=== Verification ===")
val crimeVerify = spark.read.parquet(s"$cleanPrefix/crime")
println(s"Crime cleaned records:      ${crimeVerify.count()}")
crimeVerify.printSchema()

val restVerify = spark.read.parquet(s"$cleanPrefix/restaurants")
println(s"Restaurant cleaned records:  ${restVerify.count()}")
restVerify.printSchema()

println("\n=== FirstCode.scala Complete ===")
