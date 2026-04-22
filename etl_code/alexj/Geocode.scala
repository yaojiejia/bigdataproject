import java.io.{BufferedReader, InputStreamReader}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.locationtech.jts.geom.{
  Coordinate,
  Envelope,
  Geometry,
  GeometryFactory,
  LinearRing,
  Polygon
}

// ---- Path resolution -------------------------------------------------------

val dataRoot    = sys.env.getOrElse("DATA_ROOT", "./data")
val hdfsUser    = sys.env.getOrElse("HDFS_USER", "")
val cleanPrefix = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/cleaned"  else s"$dataRoot/cleaned"
val enrPrefix   = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/enriched" else s"$dataRoot/enriched"
val geoPrefix   = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/geo"      else s"$dataRoot/geo"

val cleanCrime       = s"$cleanPrefix/crime"
val cleanRestaurants = s"$cleanPrefix/restaurants"
val cleanComplaints  = s"$cleanPrefix/complaints311"
val cleanRent        = s"$cleanPrefix/rent"

val enrCrime       = s"$enrPrefix/crime"
val enrRestaurants = s"$enrPrefix/restaurants"
val enrComplaints  = s"$enrPrefix/complaints311"
val enrRent        = s"$enrPrefix/rent"

val ntaGeojson = s"$geoPrefix/nta.geojson"
val zipToNta   = s"$geoPrefix/zip_to_nta.csv"

println(s"[geocode] DATA_ROOT=$dataRoot  HDFS_USER=${if (hdfsUser.nonEmpty) hdfsUser else "(local)"}")
println(s"[geocode] reading cleaned parquet from $cleanPrefix")
println(s"[geocode] writing enriched parquet to  $enrPrefix")

// Clean.scala wrote the rent parquet, features downstream reads our outputs,
// and the whole pipeline stays happy with nanosecond-precision timestamps.
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

// ---- Hadoop FS helper: read the GeoJSON whether it sits on local, HDFS, or GCS.

def readTextFromFs(path: String): String = {
  val hPath = new HPath(path)
  val fs    = hPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
  val in    = fs.open(hPath)
  try {
    val reader = new BufferedReader(new InputStreamReader(in, "UTF-8"))
    val sb     = new StringBuilder()
    var line   = reader.readLine()
    while (line != null) {
      sb.append(line).append('\n')
      line = reader.readLine()
    }
    sb.toString
  } finally in.close()
}

// ---- GeoJSON -> JTS polygons ----------------------------------------------
//
// NTA2020 features expose the code under ntacode/ntacode2020/NTA2020 and the
// name under ntaname/NTAName. We probe all known variants so the same job
// handles NYC DCP exports from both Open Data (9nt8-h7nd) and bytes of
// geojson sitting in a GCS bucket from a previous pipeline run.

case class NtaPolygon(code: String, name: String, geom: Geometry) extends Serializable

val NTA_CODE_FIELDS = Seq("ntacode", "NTACode", "ntacode2020", "NTA2020", "nta2020", "nta_code")
val NTA_NAME_FIELDS = Seq("ntaname", "NTAName", "nta_name", "NTA_NAME")

def findField(node: JsonNode, keys: Seq[String]): Option[String] =
  keys.flatMap(k => Option(node.get(k))).find(!_.isNull).map(_.asText())

def buildLinearRing(gf: GeometryFactory, ring: JsonNode): LinearRing = {
  // Each ring is an array of [lon, lat] coordinate pairs.
  val coords = (0 until ring.size()).map { i =>
    val pt = ring.get(i)
    new Coordinate(pt.get(0).asDouble(), pt.get(1).asDouble())
  }.toArray
  gf.createLinearRing(coords)
}

def buildPolygonFromCoords(gf: GeometryFactory, coords: JsonNode): Polygon = {
  // Polygon coords: [[outer ring], [hole1], [hole2], ...]
  val rings = (0 until coords.size()).map(i => buildLinearRing(gf, coords.get(i)))
  val shell = rings.head
  val holes = rings.tail.toArray
  gf.createPolygon(shell, holes)
}

def parseNtaGeojson(path: String): Array[NtaPolygon] = {
  val text   = readTextFromFs(path)
  val mapper = new ObjectMapper()
  val root   = mapper.readTree(text)
  val feats  = root.get("features")
  val gf     = new GeometryFactory()
  val out    = scala.collection.mutable.ArrayBuffer.empty[NtaPolygon]
  (0 until feats.size()).foreach { i =>
    val f        = feats.get(i)
    val props    = f.get("properties")
    val geom     = f.get("geometry")
    val code     = findField(props, NTA_CODE_FIELDS).orNull
    val name     = findField(props, NTA_NAME_FIELDS).orNull
    val geomType = geom.get("type").asText()
    val coords   = geom.get("coordinates")
    val built: Geometry = geomType match {
      case "Polygon"      => buildPolygonFromCoords(gf, coords)
      case "MultiPolygon" =>
        val polys = (0 until coords.size()).map(j =>
          buildPolygonFromCoords(gf, coords.get(j))
        ).toArray
        gf.createMultiPolygon(polys)
      case other =>
        println(s"[warn] skipping feature with unsupported geometry '$other'")
        null
    }
    if (built != null && code != null) {
      out += NtaPolygon(code, Option(name).getOrElse(code), built)
    }
  }
  out.toArray
}

val ntaPolygons: Array[NtaPolygon] = parseNtaGeojson(ntaGeojson)
println(f"[geocode] loaded ${ntaPolygons.length}%,d NTA polygons from $ntaGeojson")

val polysBc: Broadcast[Array[NtaPolygon]] =
  spark.sparkContext.broadcast(ntaPolygons)

// ---- Spark UDF: point -> NTA ---------------------------------------------
//
// Linear scan over ~260 polygons with an envelope pre-filter. Good enough
// for the row counts we deal with (< 1M points total); if this becomes a
// bottleneck wrap the broadcast in a JTS STRtree rebuilt per-executor.

case class NtaHit(nta_code: String, nta_name: String)

// Iteration note: first version of the UDF looped over the broadcast array
// and called `p.geom.contains(pt)` for EVERY polygon, no envelope pre-filter.
// That ran ~4x slower on the crime dataset (~16s -> ~60s geocode stage)
// because each `contains()` call walks the full polygon edges. Adding the
// envelope short-circuit made it effectively O(log m) on NYC geometry since
// ~258 of the ~260 polygons reject on envelope alone for any given point.
//
//   OLD (unoptimized, kept for posterity):
//     val pointToNta = udf { (lat: java.lang.Double, lon: java.lang.Double) =>
//       if (lat == null || lon == null) null
//       else {
//         val gf = new GeometryFactory()
//         val pt = gf.createPoint(new Coordinate(lon.doubleValue(), lat.doubleValue()))
//         polysBc.value.find(_.geom.contains(pt)).map(p => NtaHit(p.code, p.name)).orNull
//       }
//     }
//
// Even earlier (before the Scala port) this stage lived in geocode.py as:
//     import geopandas as gpd
//     gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326")
//     joined = gpd.sjoin(gdf, ntas, how="left", predicate="within")
// which ran single-process on the driver and couldn't parallelise across
// executors. Moving to a Spark broadcast UDF keeps the entire ETL surface

val pointToNta = udf { (lat: java.lang.Double, lon: java.lang.Double) =>
  if (lat == null || lon == null) null
  else {
    val gf   = new GeometryFactory()
    val pt   = gf.createPoint(new Coordinate(lon.doubleValue(), lat.doubleValue()))
    val polys = polysBc.value
    var i     = 0
    var hit: NtaHit = null
    while (i < polys.length && hit == null) {
      val p   = polys(i)
      val env = p.geom.getEnvelopeInternal
      // Fast reject by envelope then pay for the full contains().
      if (env.contains(lon.doubleValue(), lat.doubleValue()) && p.geom.contains(pt)) {
        hit = NtaHit(p.code, p.name)
      }
      i += 1
    }
    hit
  }
}

// ---- Enrich each point dataset --------------------------------------------

def enrichPoints(
    name: String,
    inPath: String,
    outPath: String,
    latCol: String = "Latitude",
    lonCol: String = "Longitude"
): DataFrame = {
  val df = spark.read.parquet(inPath)
  val before = df.count()
  val filtered = df.filter(col(latCol).isNotNull && col(lonCol).isNotNull)
  val enriched = filtered
    .withColumn("nta", pointToNta(col(latCol).cast("double"), col(lonCol).cast("double")))
    .withColumn("nta_code", col("nta.nta_code"))
    .withColumn("nta_name", col("nta.nta_name"))
    .drop("nta")
  val joined = enriched.filter(col("nta_code").isNotNull).count()
  val hitPct = 100.0 * joined / math.max(before, 1L)
  println(f"[$name] $before%,d rows, $joined%,d joined to an NTA ($hitPct%.1f%%)")
  enriched.write.mode("overwrite").parquet(outPath)
  enriched
}

println("\n=== Enriching crime ===")
enrichPoints("crime", cleanCrime, enrCrime)

println("\n=== Enriching restaurants ===")
val rest = enrichPoints("restaurants", cleanRestaurants, enrRestaurants)

println("\n=== Enriching 311 ===")
enrichPoints("311 food", cleanComplaints, enrComplaints)

// ---- ZIP -> NTA lookup via the (now enriched) restaurant dataset ---------
//
// For each ZIP, pick the NTA that contains the most restaurants. Written as
// CSV for parity with the previous Python pipeline and easy manual review;
// the rent join re-reads it as a Spark DataFrame.

println("\n=== Building ZIP -> NTA lookup ===")
val zipNta = rest
  .select(
    regexp_extract(col("ZIPCODE").cast("string"), "(\\d{5})", 1).as("ZIPCODE"),
    col("nta_code"),
    col("nta_name")
  )
  .filter(col("ZIPCODE") =!= "" && col("nta_code").isNotNull)


val modal = zipNta
  .groupBy("ZIPCODE", "nta_code", "nta_name")
  .agg(count(lit(1)).as("n"))
  .withColumn("rn", row_number().over(
    Window.partitionBy("ZIPCODE").orderBy(col("n").desc)
  ))
  .filter(col("rn") === 1)
  .select("ZIPCODE", "nta_code", "nta_name")

modal.coalesce(1)
  .write.mode("overwrite")
  .option("header", "true")
  .csv(zipToNta + ".tmp_dir")

.
def hoistSinglePart(srcDir: String, dstFile: String, ext: String): Unit = {
  val fs  = new HPath(srcDir).getFileSystem(spark.sparkContext.hadoopConfiguration)
  val dir = new HPath(srcDir)
  val parts = fs.listStatus(dir).filter { s =>
    val nm = s.getPath.getName
    nm.startsWith("part-") && nm.endsWith(ext)
  }
  require(parts.length == 1, s"expected one part$ext in $srcDir, got ${parts.length}")
  val dst = new HPath(dstFile)
  if (fs.exists(dst)) fs.delete(dst, true)
  fs.rename(parts(0).getPath, dst)
  fs.delete(dir, true)
}

hoistSinglePart(zipToNta + ".tmp_dir", zipToNta, ".csv")
println(s"[zip->nta] wrote $zipToNta (${modal.count()} ZIPs)")

// ---- Enrich rent via ZIP -> NTA ------------------------------------------

println("\n=== Enriching rent ===")
val rentDf = spark.read.parquet(cleanRent)
  .withColumn("ZIPCODE", regexp_extract(col("ZIPCODE").cast("string"), "(\\d{5})", 1))

val zipNtaDf = spark.read.option("header", true).csv(zipToNta)
val rentEnriched = rentDf.join(broadcast(zipNtaDf), Seq("ZIPCODE"), "left")
val rentBefore = rentDf.count()
val rentHit    = rentEnriched.filter(col("nta_code").isNotNull).count()
val rentPct    = 100.0 * rentHit / math.max(rentBefore, 1L)
println(f"[rent] $rentBefore%,d rows, $rentHit%,d joined to an NTA ($rentPct%.1f%%)")
rentEnriched.write.mode("overwrite").parquet(enrRent)

println("\n=== Geocode.scala complete ===")
System.exit(0)
