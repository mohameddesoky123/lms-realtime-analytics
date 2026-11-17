from pyspark.sql import SparkSession, functions as F, types as T
import psycopg2
from psycopg2.extras import execute_values

spark = (SparkSession.builder.appName("dropoff_analysis")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("course_id", T.StringType()),
    T.StructField("module_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("video_position_sec", T.IntegerType()),
    T.StructField("buffer_duration_ms", T.IntegerType()),
    T.StructField("quiz_score", T.DoubleType()),
    T.StructField("completed", T.BooleanType()),
    T.StructField("device", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("processed_at", T.StringType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "lms_clean")
       .option("startingOffsets", "earliest")
       .load())

events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("ts", F.to_timestamp("event_time"))
          .filter(F.col("ts").isNotNull())
          .filter(F.col("event_type") == "session_end")
          .filter(F.col("completed").isNotNull())
          .withWatermark("ts", "2 minutes"))

iso = lambda c: F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss'Z'")

by_position = (events
    .filter(F.col("completed") == F.lit(False))
    .withColumn("position_bucket_sec",
                (F.floor(F.col("video_position_sec") / 30) * 30).cast("int"))
    .groupBy(F.window("ts", "5 minutes"), "course_id", "module_id", "position_bucket_sec")
    .agg(F.count("*").alias("dropoff_count"))
    .select(iso(F.col("window.start")).alias("window_start"),
            iso(F.col("window.end")).alias("window_end"),
            "course_id", "module_id", "position_bucket_sec", "dropoff_count"))

by_module = (events
    .groupBy(F.window("ts", "5 minutes"), "course_id", "module_id")
    .agg(F.count("*").alias("sessions_ended"),
         F.sum(F.when(F.col("completed") == F.lit(False), 1).otherwise(0)).alias("sessions_dropped"))
    .withColumn("dropoff_rate",
                F.when(F.col("sessions_ended") > 0,
                       F.col("sessions_dropped") / F.col("sessions_ended"))
                 .otherwise(F.lit(0.0)))
    .select(iso(F.col("window.start")).alias("window_start"),
            iso(F.col("window.end")).alias("window_end"),
            "course_id", "module_id", "sessions_ended", "sessions_dropped", "dropoff_rate"))

SQL_POS = """
INSERT INTO dropoff_by_position
 (window_start, window_end, course_id, module_id, position_bucket_sec, dropoff_count)
VALUES %s
ON CONFLICT (window_start, course_id, module_id, position_bucket_sec)
DO UPDATE SET dropoff_count = EXCLUDED.dropoff_count, updated_at = now()
"""
SQL_MOD = """
INSERT INTO dropoff_by_module
 (window_start, window_end, course_id, module_id,
  sessions_ended, sessions_dropped, dropoff_rate)
VALUES %s
ON CONFLICT (window_start, course_id, module_id)
DO UPDATE SET sessions_ended = EXCLUDED.sessions_ended,
              sessions_dropped = EXCLUDED.sessions_dropped,
              dropoff_rate = EXCLUDED.dropoff_rate, updated_at = now()
"""

def make_writer(sql):
    def write_batch(df, batch_id):
        rows = [tuple(r) for r in df.collect()]
        if not rows:
            return
        conn = psycopg2.connect(host="localhost", dbname="lms", user="lms", password="lms")
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, sql, rows)
            print("Wrote batch", batch_id, "rows:", len(rows), flush=True)
        finally:
            conn.close()
    return write_batch

(by_position.writeStream.outputMode("update")
   .foreachBatch(make_writer(SQL_POS))
   .option("checkpointLocation", "4_spark_dropoff/checkpoints/dropoff_by_position")
   .trigger(processingTime="10 seconds").start())

(by_module.writeStream.outputMode("update")
   .foreachBatch(make_writer(SQL_MOD))
   .option("checkpointLocation", "4_spark_dropoff/checkpoints/dropoff_by_module")
   .trigger(processingTime="10 seconds").start())

spark.streams.awaitAnyTermination()
