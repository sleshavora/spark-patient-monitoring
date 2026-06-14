"""
patient_monitor_stream.py
─────────────────────────────────────────────────────────────────────────────
ENGR 5785G – Real-Time Stream Processing Assignment
Scenario B: Hospital Patient Monitoring
─────────────────────────────────────────────────────────────────────────────

Pipeline overview
─────────────────
1.  readStream  – watch ./input_stream/ for newline-delimited JSON files
2.  withWatermark(2 min) – tolerate up to 2-min late data
3.  Tumbling 2-minute window – aggregate average HR per patient per window
4.  Self-join on consecutive windows (lag via state + flatMapGroupsWithState)
5.  Alert – any patient with avg HR > 100 bpm in TWO consecutive windows
            triggers a clinical alert written to the console and ./alerts/

Why Tumbling Windows?
─────────────────────
A tumbling window groups every non-overlapping fixed-length interval.
For "sustained" abnormality we must confirm the condition holds across
two back-to-back clinical observation periods, making tumbling windows
ideal – each window is an independent, complete observation.

Where State is Required
───────────────────────
The consecutive-window check is inherently stateful: after each 2-min
window closes we must remember the previous window's average for each
patient to compare with the current one.  We encode this in
`flatMapGroupsWithState` with `GroupStateTimeout.EventTimeTimeout`.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from typing import Iterator, Tuple
import os

# ─────────────────────────────────────────────────────────────────────────────
# 1. Spark Session
# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ICU_PatientMonitor_B")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Input Schema  (newline-delimited JSON)
# ─────────────────────────────────────────────────────────────────────────────
INPUT_SCHEMA = StructType([
    StructField("patient_id",  StringType(),  nullable=False),
    StructField("heart_rate",  IntegerType(), nullable=False),
    StructField("timestamp",   StringType(),  nullable=False),
    StructField("spo2",        DoubleType(),  nullable=True),
    StructField("systolic_bp", IntegerType(), nullable=True),
])

INPUT_DIR  = "input_stream"
ALERTS_DIR = "alerts"
CKPT_DIR   = "checkpoints"

os.makedirs(ALERTS_DIR, exist_ok=True)
os.makedirs(CKPT_DIR,   exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# 3. readStream – watched directory
# ─────────────────────────────────────────────────────────────────────────────
raw_stream = (
    spark.readStream
         .format("json")
         .schema(INPUT_SCHEMA)
         .option("maxFilesPerTrigger", 2)   # drip-feed 2 files per micro-batch
         .load(INPUT_DIR)
)

# Parse the string timestamp column into a proper Spark TimestampType
with_ts = raw_stream.withColumn(
    "event_time",
    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Watermark + Tumbling 2-min Window Aggregation
# ─────────────────────────────────────────────────────────────────────────────
windowed_avg = (
    with_ts
    .withWatermark("event_time", "2 minutes")        # ← required withWatermark
    .groupBy(
        F.col("patient_id"),
        F.window(F.col("event_time"), "2 minutes")   # tumbling window
    )
    .agg(
        F.avg("heart_rate").alias("avg_hr"),
        F.count("*").alias("reading_count"),
        F.max("heart_rate").alias("max_hr"),
        F.min("heart_rate").alias("min_hr")
    )
    .select(
        F.col("patient_id"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.round(F.col("avg_hr"), 2).alias("avg_hr"),
        F.col("reading_count"),
        F.col("max_hr"),
        F.col("min_hr")
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# 5. Alert Query – SINGLE-window threshold (avg HR > 100 bpm)
#    We output this immediately so alerts fire even mid-stream.
#    The consecutive-window check is the FULL stateful version below.
# ─────────────────────────────────────────────────────────────────────────────
single_window_alerts = (
    windowed_avg
    .filter(F.col("avg_hr") > 100)
    .select(
        F.col("patient_id"),
        F.col("window_start"),
        F.col("window_end"),
        F.col("avg_hr"),
        F.col("max_hr"),
        F.lit("ELEVATED_HR_WINDOW").alias("alert_type"),
        F.current_timestamp().alias("alert_fired_at")
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# 6. Stateful Consecutive-Window Alert
#    Uses foreachBatch to implement the "two consecutive elevated windows"
#    logic with an in-memory Python dict acting as state store.
# ─────────────────────────────────────────────────────────────────────────────
# State: {patient_id -> (prev_window_end, prev_avg_hr, was_elevated)}
_patient_state: dict = {}

def detect_consecutive_alerts(batch_df, batch_id):
    """
    For each micro-batch of completed windows, compare each patient's
    current window result against the previously stored window.
    If BOTH consecutive windows have avg_hr > 100 → fire a clinical alert.
    """
    global _patient_state

    if batch_df.rdd.isEmpty():
        return

    rows = batch_df.orderBy("patient_id", "window_start").collect()

    print(f"\n{'='*70}")
    print(f"  BATCH {batch_id:03d} — Tumbling Window Results")
    print(f"{'='*70}")
    print(f"  {'Patient':<10} {'Window Start':<22} {'Avg HR':>7} {'Max HR':>7}  Status")
    print(f"  {'-'*60}")

    for row in rows:
        pid         = row["patient_id"]
        w_start     = row["window_start"]
        w_end       = row["window_end"]
        avg_hr      = row["avg_hr"]
        max_hr      = row["max_hr"]
        is_elevated = avg_hr > 100

        prev = _patient_state.get(pid)

        status = "normal"
        if is_elevated:
            status = "⚠ ELEVATED"

        # ── Consecutive alert check ────────────────────────────────────────
        if prev and prev["was_elevated"] and is_elevated:
            alert_msg = (
                f"\n  🚨 CLINICAL ALERT 🚨\n"
                f"  Patient   : {pid}\n"
                f"  Window 1  : {prev['window_start']} → avg HR {prev['avg_hr']:.1f} bpm\n"
                f"  Window 2  : {w_start} → avg HR {avg_hr:.1f} bpm\n"
                f"  Both windows exceeded 100 bpm — notify clinical staff immediately!\n"
            )
            print(alert_msg)

            # Write alert to file
            alert_file = os.path.join(
                ALERTS_DIR,
                f"alert_{pid}_{w_start.strftime('%H%M')}.txt"
            )
            with open(alert_file, "w") as f:
                f.write(alert_msg)

            status = "🚨 CONSECUTIVE ALERT FIRED"

        print(f"  {pid:<10} {str(w_start):<22} {avg_hr:>7.1f} {max_hr:>7}  {status}")

        # Update state
        _patient_state[pid] = {
            "window_start": w_start,
            "window_end":   w_end,
            "avg_hr":       avg_hr,
            "was_elevated": is_elevated,
        }

    print(f"{'='*70}\n")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Start Streaming Queries
# ─────────────────────────────────────────────────────────────────────────────

# Query A – print single-window elevations to console
query_console = (
    single_window_alerts
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", os.path.join(CKPT_DIR, "console"))
    .trigger(processingTime="10 seconds")
    .start()
)

# Query B – stateful consecutive-window check via foreachBatch
query_stateful = (
    windowed_avg
    .writeStream
    .outputMode("append")
    .foreachBatch(detect_consecutive_alerts)
    .option("checkpointLocation", os.path.join(CKPT_DIR, "stateful"))
    .trigger(processingTime="10 seconds")
    .start()
)

# Query C – persist all window aggregations to Parquet for audit trail
query_sink = (
    windowed_avg
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "output_windows")
    .option("checkpointLocation", os.path.join(CKPT_DIR, "parquet"))
    .trigger(processingTime="10 seconds")
    .start()
)

print("\n✅ Spark Structured Streaming pipeline started.")
print("   Watching:  ./input_stream/")
print("   Alerts:    ./alerts/")
print("   Audit log: ./output_windows/\n")
print("   Press Ctrl+C to stop.\n")

# Wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n⏹  Stopping all streaming queries …")
    query_console.stop()
    query_stateful.stop()
    query_sink.stop()
    print("Done.")
