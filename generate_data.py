"""
generate_data.py
Generates a synthetic IoMT patient heart-rate dataset and writes
individual JSON files into ./input_stream/ to simulate streaming.
"""
import json, os, random, time
from datetime import datetime, timedelta

random.seed(42)
PATIENTS = [f"P{str(i).zfill(3)}" for i in range(1, 11)]   # P001 – P010
OUT_DIR  = "input_stream"
os.makedirs(OUT_DIR, exist_ok=True)

# ── Scenario: two patients (P001, P007) will have sustained elevated HR ──
ELEVATED = {"P001", "P007"}

base_time = datetime(2024, 6, 1, 8, 0, 0)

rows = []
for minute in range(0, 10):          # 10 minutes of data
    for patient in PATIENTS:
        for second in range(0, 60, 5):    # reading every 5 s
            ts = base_time + timedelta(minutes=minute, seconds=second)
            if patient in ELEVATED:
                hr = random.randint(102, 130)   # consistently above 100
            else:
                hr = random.randint(60, 98)     # normal range
            rows.append({
                "patient_id":  patient,
                "heart_rate":  hr,
                "timestamp":   ts.strftime("%Y-%m-%d %H:%M:%S"),
                "spo2":        round(random.uniform(95, 100), 1),
                "systolic_bp": random.randint(110, 140)
            })

# Write one JSON file per minute-batch to simulate micro-batch streaming
batch_size = len(PATIENTS) * 12       # 12 readings/patient/minute
for i, batch_start in enumerate(range(0, len(rows), batch_size)):
    batch = rows[batch_start: batch_start + batch_size]
    fname = os.path.join(OUT_DIR, f"batch_{i:04d}.json")
    with open(fname, "w") as f:
        for record in batch:
            f.write(json.dumps(record) + "\n")

print(f"Generated {len(rows):,} records across {i+1} files in ./{OUT_DIR}/")
