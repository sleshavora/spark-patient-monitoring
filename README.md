# 🏥 Real-Time ICU Patient Heart-Rate Monitor
### ENGR 5785G — Real-Time Data Analytics for IoT | Stream Processing Assignment

---

## Scenario B — Hospital Patient Monitoring

> **Goal:** Detect *sustained* abnormal heart rates — not single spikes —  
> across ICU patient streams using **Tumbling 2-minute windows**.  
> A clinical alert fires when a patient's average HR exceeds 100 bpm  
> in **two consecutive** tumbling windows.

---

## Repository Structure

```
spark-patient-monitoring/
├── patient_monitor_stream.py   # ← Main Spark Structured Streaming job
├── generate_data.py            # ← Synthetic IoMT dataset generator
├── requirements.txt            # ← Python dependencies
├── input_stream/               # ← JSON files arrive here (watched directory)
├── alerts/                     # ← Written alert files per patient per window
├── output_windows/             # ← Parquet audit sink (all window aggregations)
├── checkpoints/                # ← Spark streaming checkpoints
└── README.md
```

---

## Dataset

**Source:** [IoMT Health Monitoring Dataset (Kaggle)](https://www.kaggle.com/datasets/messif/iomt-health-monitoring)  
**Format:** Newline-delimited JSON, one file per minute-batch  
**Fields used:**

| Field         | Type      | Description                         |
|---------------|-----------|-------------------------------------|
| `patient_id`  | String    | Unique ICU patient identifier       |
| `heart_rate`  | Integer   | BPM reading from IoT wearable       |
| `timestamp`   | String    | Event time `YYYY-MM-DD HH:MM:SS`    |
| `spo2`        | Double    | Blood oxygen saturation (%)         |
| `systolic_bp` | Integer   | Systolic blood pressure (mmHg)      |

A synthetic version with 10 patients and 1,200 records is generated  
by `generate_data.py` for local testing.

---

## How to Run

### Prerequisites
- Python ≥ 3.9  
- Java 8 or 11 (required by Spark)  
- Apache Spark 3.5 (auto-installed via pip)

### 1 — Clone and install
```bash
git clone https://github.com/<your-username>/spark-patient-monitoring.git
cd spark-patient-monitoring
pip install -r requirements.txt
```

### 2 — Generate the streaming dataset
```bash
python generate_data.py
# Output: 10 JSON files written to ./input_stream/
```

### 3 — Start the streaming job
```bash
python patient_monitor_stream.py
```

Spark will watch `./input_stream/` and process files in micro-batches  
every 10 seconds (`maxFilesPerTrigger=2`).

### 4 — Observe output
- **Console** — Batch results and 🚨 alerts printed to terminal  
- **`./alerts/`** — One `.txt` file per fired consecutive-window alert  
- **`./output_windows/`** — All window aggregations in Parquet format  

### 5 — Stop
Press `Ctrl+C` to gracefully shut down all streaming queries.

---

## Pipeline Architecture

```
./input_stream/          readStream (JSON, watched dir)
      │
      ▼
  Parse timestamp ──► withWatermark("event_time", "2 minutes")
      │
      ▼
  window("event_time", "2 minutes")   ← Tumbling window
  groupBy(patient_id, window)
  avg(heart_rate)
      │
      ├──► Query A: filter avg_hr > 100 → console (single-window alert)
      │
      ├──► Query B: foreachBatch stateful check
      │             compare current window vs prev stored state
      │             if BOTH elevated → 🚨 CLINICAL ALERT
      │
      └──► Query C: parquet sink (audit trail)
```

---

## Expected Console Output

```
======================================================================
  BATCH 003 — Tumbling Window Results
======================================================================
  Patient    Window Start            Avg HR  Max HR  Status
  ------------------------------------------------------------

  🚨 CLINICAL ALERT 🚨
  Patient   : P001
  Window 1  : 2024-06-01 08:00:00 → avg HR 116.7 bpm
  Window 2  : 2024-06-01 08:02:00 → avg HR 118.5 bpm
  Both windows exceeded 100 bpm — notify clinical staff immediately!

  P001       2024-06-01 08:02:00      118.5     129  🚨 CONSECUTIVE ALERT FIRED
  P002       2024-06-01 08:02:00       77.9      94  normal
  ...
  P007       2024-06-01 08:02:00      118.5     130  🚨 CONSECUTIVE ALERT FIRED
======================================================================
```

---

## Written Explanation

### Why This Window Type?

**Tumbling (non-overlapping) 2-minute windows** are the correct choice  
for this clinical scenario for three reasons:

1. **Clinical observation periods are discrete.** ICU nursing protocols  
   assess patient vitals over fixed observation intervals (e.g., every  
   2 minutes), not on a rolling basis. A tumbling window mirrors this  
   real-world workflow exactly.

2. **Eliminating double-counting.** Because tumbling windows never  
   overlap, each heart-rate reading belongs to exactly one window.  
   This prevents a single brief spike from inflating two adjacent  
   aggregates and causing false consecutive alerts — which would happen  
   with sliding windows.

3. **"Sustained" requires independent confirmation.** The assignment  
   specifically targets *sustained* abnormality, not transient spikes.  
   Confirming that the average exceeds 100 bpm in *two independent,  
   back-to-back* windows is only meaningful if those windows are truly  
   non-overlapping. If we used a sliding window (e.g., 4 min / 1 min),  
   consecutive windows would share ~75% of their data, making them  
   nearly redundant as an independence check.

### Where the Pipeline Requires State

This pipeline has **two layers of state**:

| Layer | What is stored | Why it is needed |
|---|---|---|
| **Spark internal (watermark state)** | Partial window aggregates that may receive late-arriving events (up to 2 min) | Spark must buffer incomplete windows until the watermark advances past `window_end`, at which point they are emitted and state is evicted |
| **Application state (`_patient_state` dict)** | Each patient's *previous* window result: `{window_start, avg_hr, was_elevated}` | After a window closes and its aggregate is emitted, we must remember whether it was elevated so we can compare it with the *next* window — this cross-window memory is the definition of stateful stream processing |

Without the application-level state store, we can only check each  
window in isolation; we cannot detect *consecutive* violations across  
window boundaries.

---

## Alert Conditions

| Alert | Trigger | Output |
|---|---|---|
| `ELEVATED_HR_WINDOW` | avg HR > 100 bpm in any single 2-min window | Console (Spark DataFrame sink) |
| `CLINICAL ALERT` | avg HR > 100 bpm in **two consecutive** 2-min windows | Console + `./alerts/alert_<patient>_<time>.txt` |

---

## Technical Checklist (per assignment requirements)

- [x] `readStream` with watched directory (`./input_stream/`)  
- [x] `withWatermark("event_time", "2 minutes")`  
- [x] Window aggregation: `window("event_time", "2 minutes")` — tumbling  
- [x] Alert condition defined and triggered as filtered output stream  
- [x] Short written explanation (see above and inline docstrings)  
- [x] Screenshot of alert output firing in Spark console (see `/screenshots/`)

---

## Author
Student Name — ENGR 5785G, Ontario Tech University
