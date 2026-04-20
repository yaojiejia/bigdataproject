// ============================================================
// Consumer.scala — Spark Structured Streaming consumer for 311 complaints.
// Run with:
//   spark-shell --master local[*] \
//     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
//     -i Consumer.scala
// ============================================================
//
// Reads Kafka topic `complaints311`, parses the JSON payload, watermarks at
// 10 minutes, and produces 5-minute tumbling windows keyed by
// (nta_code, nta_name, complaint_type). Two sinks:
//
//   1. data/stream/windowed/  — append-mode Parquet history of every window.
//   2. data/stream/latest.parquet — snapshot the API reads for the live feed.
//      Written via foreachBatch + atomic-ish rename so the API never sees a
//      half-written directory.
//
// Iteration note: an earlier version started the streaming query without
// pre-creating the Kafka topic. The broker has auto-create enabled, but that
// only fires on first produce, so a consumer starting before the producer hit
// UnknownTopicOrPartitionException three times and the query died. ensureTopic
// below uses the Kafka AdminClient (shipped with the spark-sql-kafka package)
// to create the topic idempotently at startup.
// ============================================================

import java.nio.file.{Files, Path => JPath, Paths, StandardCopyOption}
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
val topic     = "complaints311"

val dataRoot       = sys.env.getOrElse("DATA_ROOT", "./data")
val streamRoot     = s"$dataRoot/stream"
val streamWindowed = s"$streamRoot/windowed"
val streamLatest   = s"$streamRoot/latest.parquet"
val streamLogs     = s"$streamRoot/logs"

// Make sure output dirs exist so foreachBatch doesn't race on first run.
Seq(streamRoot, streamWindowed, streamLogs).foreach { p =>
  new java.io.File(p).mkdirs()
}

// ---- Kafka admin: create topic if missing ---------------------------------

def ensureTopic(): Unit = {
  val props = new Properties()
  props.put("bootstrap.servers", bootstrap)
  val admin = AdminClient.create(props)
  try {
    val newTopic = new NewTopic(topic, 1, 1.toShort)
    try {
      admin.createTopics(java.util.Collections.singletonList(newTopic)).all().get()
      println(s"[consumer] created topic '$topic'")
    } catch {
      case e: java.util.concurrent.ExecutionException if e.getCause.isInstanceOf[TopicExistsException] =>
        println(s"[consumer] topic '$topic' already exists")
      case _: TopicExistsException =>
        println(s"[consumer] topic '$topic' already exists")
    }
  } finally {
    admin.close()
  }
}

ensureTopic()

// ---- Schema of the JSON payload -------------------------------------------

val schema = StructType(Seq(
  StructField("COMPLAINT_ID",   StringType),
  StructField("COMPLAINT_TYPE", StringType),
  StructField("DESCRIPTOR",     StringType),
  StructField("ZIPCODE",        StringType),
  StructField("BORO",           StringType),
  StructField("Latitude",       DoubleType),
  StructField("Longitude",      DoubleType),
  StructField("nta_code",       StringType),
  StructField("nta_name",       StringType),
  StructField("created_date",   StringType)
))

// ---- Read + window ---------------------------------------------------------

val raw = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrap)
  .option("subscribe", topic)
  .option("startingOffsets", "latest")
  .load()

val parsed = raw
  .select(
    from_json(col("value").cast("string"), schema).as("j"),
    col("timestamp").as("kafka_ts")
  )
  .select(col("j.*"), col("kafka_ts"))
  .withColumn("event_time", to_timestamp(col("created_date")))
  .withColumn("event_time", coalesce(col("event_time"), col("kafka_ts")))

val windowed = parsed
  .withWatermark("event_time", "10 minutes")
  .groupBy(
    window(col("event_time"), "5 minutes").as("window"),
    col("nta_code"),
    col("nta_name"),
    col("COMPLAINT_TYPE").as("complaint_type")
  )
  .agg(count(lit(1)).as("count"))
  .select(
    col("window.start").as("window_start"),
    col("window.end").as("window_end"),
    col("nta_code"), col("nta_name"),
    col("complaint_type"), col("count")
  )

// ---- foreachBatch: atomic-ish snapshot of the latest per-NTA counts -------

def writeLatest(batchDf: DataFrame, batchId: Long): Unit = {
  if (batchDf.rdd.isEmpty()) return
  val agg = batchDf.groupBy("nta_code", "nta_name").agg(
    sum("count").as("n_complaints_recent"),
    max("window_end").as("window_end")
  )
  val tmpPath = s"$streamRoot/latest.tmp-$batchId.parquet"
  agg.coalesce(1).write.mode("overwrite").parquet(tmpPath)

  val latestJ = Paths.get(streamLatest)
  val tmpJ    = Paths.get(tmpPath)
  if (Files.exists(latestJ)) {
    // rmtree (directory) — Java doesn't have a one-liner, so walk.
    def rmrf(p: JPath): Unit = {
      if (Files.isDirectory(p)) {
        val it = Files.newDirectoryStream(p)
        try it.asScala.foreach(rmrf) finally it.close()
      }
      Files.deleteIfExists(p)
    }
    rmrf(latestJ)
  }
  Files.move(tmpJ, latestJ, StandardCopyOption.ATOMIC_MOVE)
  println(s"[batch $batchId] latest.parquet updated; ${agg.count()} NTAs")
}

// ---- Launch both sinks ----------------------------------------------------

val history = windowed.writeStream
  .format("parquet")
  .option("path", streamWindowed)
  .option("checkpointLocation", s"$streamLogs/history-ck")
  .outputMode("append")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()

val latest = windowed.writeStream
  .outputMode("update")
  .option("checkpointLocation", s"$streamLogs/latest-ck")
  .foreachBatch(writeLatest _)
  .trigger(Trigger.ProcessingTime("15 seconds"))
  .start()

println("[consumer] streaming; Ctrl-C to stop")
spark.streams.awaitAnyTermination()
