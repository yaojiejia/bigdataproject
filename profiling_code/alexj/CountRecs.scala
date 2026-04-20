// ============================================================
// CountRecs.scala — Data Exploration on Dataproc (spark-shell)
// Run with:  :load CountRecs.scala
// ============================================================

import org.apache.spark.sql.functions._

// --- 1. Load datasets as DataFrames ---
println("=== Loading Datasets ===")

// Local-first: reads from $DATA_ROOT/raw (default ./data/raw) unless
// HDFS_USER is set, in which case falls back to Dataproc HDFS paths.
val dataRoot = sys.env.getOrElse("DATA_ROOT", "./data") + "/raw"
val hdfsUser = sys.env.getOrElse("HDFS_USER", "")
val crimePath = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/data/nypd_complaints.csv" else s"$dataRoot/nypd_complaints.csv"
val restPath  = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/data/restaurant_inspections.csv" else s"$dataRoot/restaurant_inspections.csv"

val crime = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", "\"").csv(crimePath)

val restaurants = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", "\"").csv(restPath)

// --- 2. Print schemas to understand the data ---
println("\n=== Crime Data Schema ===")
crime.printSchema()

println("\n=== Restaurant Inspections Schema ===")
restaurants.printSchema()

// --- 3. Count the number of records ---
println("\n=== Record Counts ===")
val crimeCount = crime.count()
println(s"Crime records:                $crimeCount")

val restCount = restaurants.count()
println(s"Restaurant inspection records: $restCount")

// --- 4. Map records to (key, value) and count using map() ---
println("\n=== Map to Key-Value and Count (RDD map) ===")

val crimeMapped = crime.rdd.map(row => (row.getAs[String]("BORO_NM"), 1))
val crimeMappedCount = crimeMapped.count()
println(s"Crime mapped (Borough, 1) RDD count: $crimeMappedCount")

val restMapped = restaurants.rdd.map(row => (row.getAs[String]("BORO"), 1))
val restMappedCount = restMapped.count()
println(s"Restaurant mapped (Boro, 1) RDD count: $restMappedCount")

// --- 5. Counts per key using reduceByKey ---
println("\n=== Crime Records per Borough (reduceByKey) ===")
crimeMapped.reduceByKey(_ + _).collect().sortBy(-_._2).foreach(println)

println("\n=== Restaurant Records per Borough (reduceByKey) ===")
restMapped.reduceByKey(_ + _).collect().sortBy(-_._2).foreach(println)

// --- 6. Distinct values in analytic columns ---

// Crime: OFNS_DESC (offense description)
println("\n=== Crime: Distinct Offense Descriptions (OFNS_DESC) ===")
crime.select("OFNS_DESC").distinct().show(50, false)

// Crime: BORO_NM (borough name)
println("\n=== Crime: Distinct Boroughs (BORO_NM) ===")
crime.select("BORO_NM").distinct().show()

// Crime: LAW_CAT_CD (law category)
println("\n=== Crime: Distinct Law Categories (LAW_CAT_CD) ===")
crime.select("LAW_CAT_CD").distinct().show()

// Restaurants: BORO
println("\n=== Restaurants: Distinct Boroughs (BORO) ===")
restaurants.select("BORO").distinct().show()

// Restaurants: CUISINE DESCRIPTION
println("\n=== Restaurants: Distinct Cuisine Types ===")
restaurants.select("CUISINE DESCRIPTION").distinct().show(50, false)

// Restaurants: GRADE
println("\n=== Restaurants: Distinct Grades ===")
restaurants.select("GRADE").distinct().show()

println("\n=== CountRecs.scala Complete ===")
