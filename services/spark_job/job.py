import os
import time

import clickhouse_connect
from pyspark.sql import SparkSession, functions as F, types as T

KAFKA = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_RAW = os.environ["KAFKA_TOPIC_RAW"]
TOPIC_METRICS = os.environ["KAFKA_TOPIC_METRICS"]
CH_HOST = os.environ["CLICKHOUSE_HOST"]
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_DB = os.environ.get("CLICKHOUSE_DB", "traffic")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
WINDOW = int(os.environ.get("SPARK_WINDOW_SECONDS", "10"))
WATERMARK = int(os.environ.get("SPARK_WATERMARK_SECONDS", "2"))
STARTING = os.environ.get("SPARK_STARTING_OFFSETS", "latest")
CHECKPOINT = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/checkpoints/metrics")

COLS = [
    "run_id", "tls_id", "scenario", "mode", "window_start_ms", "window_end_ms", "sim_time_end",
    "last_produced_at_ms", "spark_out_at_ms", "avg_queue", "avg_queue_main",
    "avg_queue_side", "avg_wait", "avg_speed", "avg_occupancy",
]

RAW_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("scenario", T.StringType()),
    T.StructField("mode", T.StringType()),
    T.StructField("seed", T.LongType()),
    T.StructField("sim_time", T.DoubleType()),
    T.StructField("event_time_ms", T.LongType()),
    T.StructField("produced_at_ms", T.LongType()),
    T.StructField("tls_id", T.StringType()),
    T.StructField("total_halting", T.LongType()),
    T.StructField("queue_main", T.LongType()),
    T.StructField("queue_side", T.LongType()),
    T.StructField("avg_waiting_time", T.DoubleType()),
    T.StructField("avg_speed", T.DoubleType()),
    T.StructField("avg_occupancy", T.DoubleType()),
    T.StructField("vehicle_count", T.LongType()),
])

spark = (
    SparkSession.builder.appName("traffic-metrics")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC_RAW)
    .option("startingOffsets", STARTING)
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw.select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("d"))
    .select("d.*")
    .where(F.col("event_time_ms").isNotNull())
    .withColumn("event_time", (F.col("event_time_ms") / 1000).cast("timestamp"))
)

agg = (
    parsed.withWatermark("event_time", "%d seconds" % WATERMARK)
    .groupBy(F.window("event_time", "%d seconds" % WINDOW), "run_id", "tls_id")
    .agg(
        F.first("scenario").alias("scenario"),
        F.first("mode").alias("mode"),
        F.max("sim_time").alias("sim_time_end"),
        F.max("produced_at_ms").alias("last_produced_at_ms"),
        F.avg("total_halting").alias("avg_queue"),
        F.avg("queue_main").alias("avg_queue_main"),
        F.avg("queue_side").alias("avg_queue_side"),
        F.avg("avg_waiting_time").alias("avg_wait"),
        F.avg("avg_speed").alias("avg_speed"),
        F.avg("avg_occupancy").alias("avg_occupancy"),
    )
)

metrics = agg.select(
    "run_id",
    "tls_id",
    "scenario",
    "mode",
    (F.col("window.start").cast("double") * 1000).cast("long").alias("window_start_ms"),
    (F.col("window.end").cast("double") * 1000).cast("long").alias("window_end_ms"),
    "sim_time_end",
    "last_produced_at_ms",
    "avg_queue",
    "avg_queue_main",
    "avg_queue_side",
    "avg_wait",
    "avg_speed",
    "avg_occupancy",
)


def write_batch(batch_df, batch_id):
    out = (
        batch_df.withColumn("spark_out_at_ms", F.lit(int(time.time() * 1000)))
        .select(*COLS)
        .persist()
    )
    try:
        rows = out.collect()
        if not rows:
            return
        client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER,
            password=CH_PASSWORD, database=CH_DB,
        )
        client.insert("metrics", [list(r) for r in rows], column_names=COLS)
        client.close()
        (
            out.select(
                F.concat_ws(":", "run_id", "tls_id").alias("key"),
                F.to_json(F.struct(*COLS)).alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("topic", TOPIC_METRICS)
            .save()
        )
        print("batch %s: %d rows written" % (batch_id, len(rows)), flush=True)
    finally:
        out.unpersist()


query = (
    metrics.writeStream.outputMode("append")
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="2 seconds")
    .start()
)
query.awaitTermination()