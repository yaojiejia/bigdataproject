// ============================================================
// Producer.scala — Replay enriched 311 complaints onto Kafka.
// Run with:
//   spark-shell --master local[*] \
//     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
//     -i stream/Producer.scala
// Or just: make stream-produce
// Optional args (after `--`):
//   --rps 50     events per second (default 50)
//   --shuffle    shuffle records each pass
// ============================================================
//
// Reads `data/enriched/complaints311/` via Spark so the records already have
// their nta_code (same shape the consumer expects). Every event gets a fresh
// `created_date` so the 5-minute windowed sink in Consumer.scala never
// watermarks them out. The producer loops forever; Ctrl-C cleanly shuts it.
//
// Why Scala now:  we already use the kafka-clients jar that ships inside the
// spark-sql-kafka package for Consumer.scala's AdminClient. Using the same
// dependency chain for the producer drops the only remaining Python big-data
// script (the Python backend is gone in this branch).
// ============================================================

import java.time.Instant
import java.util.Properties

import scala.util.Random

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

// ---- CLI arg parsing (spark-shell passes scripts args via `-- -arg`). -----

val rawArgs: Array[String] = {
  // spark-shell splits on "--" and gives the trailing bit to the script via
  // sys.props("scala.args") OR they're visible on the classpath as the last
  // element of the main args. Easiest portable approach: read env vars.
  val envRps = sys.env.get("STREAM_RPS").flatMap(s => scala.util.Try(s.toDouble).toOption)
  val envShuffle = sys.env.get("STREAM_SHUFFLE").exists(_.equalsIgnoreCase("true"))
  Array.empty[String] // reserved — we drive config via env for predictability
}

val rps: Double =
  sys.env.get("STREAM_RPS").flatMap(s => scala.util.Try(s.toDouble).toOption).getOrElse(50.0)
val shuffle: Boolean =
  sys.env.get("STREAM_SHUFFLE").exists(_.equalsIgnoreCase("true"))

// ---- Paths ---------------------------------------------------------------

val dataRoot  = sys.env.getOrElse("DATA_ROOT", "./data")
val hdfsUser  = sys.env.getOrElse("HDFS_USER", "")
val enrPrefix = if (hdfsUser.nonEmpty) s"hdfs:///user/$hdfsUser/enriched" else s"$dataRoot/enriched"
val enrComplaints = s"$enrPrefix/complaints311"

val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
val topic     = "complaints311"

println(s"[producer] reading $enrComplaints")
println(s"[producer] bootstrap=$bootstrap  topic=$topic  rps=$rps  shuffle=$shuffle")

spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

// ---- Load + project -------------------------------------------------------
//
// Keep the same JSON shape the consumer's from_json() schema expects:
//   COMPLAINT_ID, COMPLAINT_TYPE, DESCRIPTOR, ZIPCODE, BORO,
//   Latitude, Longitude, nta_code, nta_name, created_date

val src = spark.read.parquet(enrComplaints)
  .filter(col("nta_code").isNotNull)

val KEEP = Seq(
  "COMPLAINT_ID", "COMPLAINT_TYPE", "DESCRIPTOR", "ZIPCODE",
  "BORO", "Latitude", "Longitude", "nta_code", "nta_name"
)
val available = KEEP.filter(src.columns.contains)
val projected = src.select(available.map(col): _*)

// Cheap way to get JSON-ready strings: let Spark build them.
val asJson = projected.select(
  col("nta_code").cast("string").as("key"),
  to_json(struct(projected.columns.map(col): _*)).as("value")
)

println("[producer] collecting records to the driver ...")
val records: Array[Row] = asJson.collect()
println(f"[producer] ${records.length}%,d records in memory")

if (records.isEmpty) {
  System.err.println("no source records found — run the batch pipeline first")
  System.exit(2)
}

// ---- Kafka producer ------------------------------------------------------

val props = new Properties()
props.put("bootstrap.servers", bootstrap)
props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("linger.ms", "10")
props.put("acks", "1")

val producer = new KafkaProducer[String, String](props)
val intervalNanos: Long = math.max(1L, (1e9 / math.max(rps, 0.1)).toLong)

// ---- Stamp `created_date` to "now" as we send ----------------------------
//
// The record already carries the *historical* incident date; rewriting it to
// now() keeps the stream watermark fresh and avoids Consumer.scala dropping
// events that arrived late relative to the watermark (10 min).
// We re-parse just enough of the JSON string to slip in created_date without
// pulling in a JSON library.

def withFreshDate(value: String): String = {
  val now = Instant.now.toString
  if (value == null || value.length < 2) value
  else if (value.endsWith("}"))
    value.substring(0, value.length - 1) + s""","created_date":"$now"}"""
  else value
}

val rng = new Random(System.currentTimeMillis())

println(s"[producer] sending @ ~$rps eps (Ctrl-C to stop)")
var sent = 0L
sys.addShutdownHook {
  println(s"\n[producer] shutdown: sent $sent events")
  producer.flush()
  producer.close(java.time.Duration.ofSeconds(2))
}

try {
  while (true) {
    val pool: Array[Int] =
      if (shuffle) rng.shuffle(records.indices.toList).toArray
      else records.indices.toArray
    var i = 0
    while (i < pool.length) {
      val row  = records(pool(i))
      val key  = row.getAs[String]("key")
      val body = withFreshDate(row.getAs[String]("value"))
      producer.send(new ProducerRecord[String, String](topic, key, body))
      sent += 1
      if (sent % 500 == 0) println(s"[producer] $sent events sent")
      // Busy-ish sleep: LockSupport.parkNanos lets us hit fractional-rps intervals.
      val target = System.nanoTime() + intervalNanos
      while (System.nanoTime() < target) {
        java.util.concurrent.locks.LockSupport.parkNanos(intervalNanos)
      }
      i += 1
    }
  }
} catch {
  case _: InterruptedException =>
    println(s"\n[producer] interrupted after $sent events")
} finally {
  producer.flush()
  producer.close(java.time.Duration.ofSeconds(2))
}

System.exit(0)
