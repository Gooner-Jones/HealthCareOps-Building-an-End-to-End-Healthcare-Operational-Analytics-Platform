# ============================================================
# SETUP
# ============================================================
%pip install faker

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType
)
from faker import Faker
from datetime import datetime, date, timedelta
import random
import uuid
import hashlib

fake = Faker("en_GB")          # en_ZA not supported; en_GB as neutral fallback
Faker.seed(42)
random.seed(42)

print("Setup complete.")

# ============================================================
# PATCH — AdmissionDischarge Bronze, Cell 1 (setup): add roster read
# ============================================================
doctor_roster_df = spark.read.table("hospital_analytics.03_gold.dim_doctor").select("doctor_id")
DOCTOR_IDS = [row["doctor_id"] for row in doctor_roster_df.collect()]

# ============================================================
# CELL 2 — REFERENCE DATA (complete — network-wide ward
# capacities, plus everything else this cell originally defined)
# ============================================================
FACILITIES = [
    "Steve Biko Academic Hospital", "Kalafong Provincial Tertiary Hospital",
    "Tshwane District Hospital", "Unitas Hospital",
    "Little Company of Mary Hospital", "Netcare Montana Hospital", "Mediclinic Kloof",
]

ADMISSION_TYPES    = ["Emergency", "Elective", "Maternity", "Referral", "Walk-in"]
ADMISSION_WEIGHTS  = [0.40, 0.25, 0.15, 0.12, 0.08]

REFERRING_SOURCES  = [
    "Self / Walk-in", "GP Referral", "Emergency Services (EMS)",
    "Transferred from Clinic", "Transferred from Another Hospital", "Specialist Referral"
]
DISCHARGE_DISPOSITIONS = [
    "Discharged Home", "Transferred to Another Facility",
    "Discharged Against Medical Advice", "Deceased",
    "Referred to Outpatient Follow-up", "Admitted to Long-term Care",
]
DISCHARGE_WEIGHTS  = [0.60, 0.12, 0.05, 0.04, 0.15, 0.04]

SOURCE_SYSTEMS     = ["MedTech EMR", "GoodX", "Healthware", "Nexus EMR", "Paper-Digitised"]

# Network-wide ward capacities (across all 7 facilities — consistent
# with dim_ward being a facility-independent ward-TYPE dimension)
NUM_FACILITIES = 7

WARD_CAPACITIES_PER_FACILITY = {
    "Casualty": 30, "ICU": 12, "General Medical": 60, "Surgical": 40,
    "Paediatrics": 35, "Maternity": 40, "Oncology": 25, "Orthopaedics": 30,
    "Psychiatry": 30, "Cardiology": 20, "Isolation / Infectious Disease": 20,
    "Outpatient": 50,
}

WARD_CAPACITIES = {
    ward: cap * NUM_FACILITIES
    for ward, cap in WARD_CAPACITIES_PER_FACILITY.items()
}
WARD_KEYS = list(WARD_CAPACITIES.keys())

MONTH_WEIGHTS = {1:1.0,2:1.0,3:1.0,4:1.0,5:1.0,6:1.5,7:1.5,8:1.0,9:1.0,10:1.0,11:1.0,12:1.0}

def random_date_with_seasonality(start_date, end_date, month_weights):
    days_list    = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    date_weights = [month_weights.get(d.month, 1.0) for d in days_list]
    return random.choices(days_list, weights=date_weights, k=1)[0]

doctor_roster_df = spark.read.table("hospital_analytics.03_gold.dim_doctor").select("doctor_id")
DOCTOR_IDS = [row["doctor_id"] for row in doctor_roster_df.collect()]

patient_df   = spark.read.table("hospital_analytics.01_bronze.patient_info").select("patient_id", "primary_diagnosis")
patient_dict = {row["patient_id"]: row["primary_diagnosis"] for row in patient_df.collect()}
patient_ids  = list(patient_dict.keys())

print("Network-wide ward capacities:")
for w, c in WARD_CAPACITIES.items():
    print(f"  {w:35s} {c}")


# ============================================================
# SEASONALITY 
# ============================================================
def random_date_with_seasonality(start_date, end_date, month_weights):
    days_list    = [start_date + timedelta(days=i)
                    for i in range((end_date - start_date).days + 1)]
    date_weights = [month_weights.get(d.month, 1.0) for d in days_list]
    return random.choices(days_list, weights=date_weights, k=1)[0]

MONTH_WEIGHTS = {
    1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0,  5: 1.0,  6: 1.5,
    7: 1.5, 8: 1.0, 9: 1.0, 10: 1.0, 11: 1.0, 12: 1.0
}
MONTHLY_CAPS = {
    5: 1333, 6: 2000, 7: 2000,
    8: 1333,  9: 1333,  10: 1333, 11: 1333
}

# ============================================================
# CELL 3 — PHASE 1: Generate admission/discharge DATES
# (fixed: replaced the unconditional 30%-never-discharged branch
# with a proper LOS distribution. The old logic created ~2,400
# permanently-active patients against only 392 total system-wide
# beds — mathematically impossible for any assignment algorithm
# to satisfy. A patient is now only "still active" if their
# (realistic) length of stay genuinely hasn't concluded by today.)
# ============================================================
today_date     = date.today()
ingestion_ts   = datetime.now()
ingestion_date = ingestion_ts.date()

target_records = 8_000
raw_events     = []
active_patients = {}
last_discharge_dict = {}

def draw_length_of_stay():
    """Realistic LOS distribution — most stays short, a small
    heavy tail for genuinely long/complex cases."""
    bucket = random.choices(
        ["short", "medium", "long", "extended"],
        weights=[0.70, 0.20, 0.08, 0.02],
        k=1
    )[0]
    if bucket == "short":
        return random.randint(1, 7)
    elif bucket == "medium":
        return random.randint(8, 21)
    elif bucket == "long":
        return random.randint(22, 45)
    else:  # extended — rare, but bounded, not permanent
        return random.randint(46, 90)

attempts = 0
while len(raw_events) < target_records:
    attempts += 1
    patient_id = random.choice(patient_ids)
    if patient_id in active_patients:
        continue

    if last_discharge_dict.get(patient_id) is not None:
        min_adm_date = last_discharge_dict[patient_id] + timedelta(days=1)
        if min_adm_date > today_date:
            min_adm_date = today_date
    else:
        six_months_ago = today_date - timedelta(days=180)
        min_adm_date   = random_date_with_seasonality(six_months_ago, today_date, MONTH_WEIGHTS)

    try:
        admission_date = random_date_with_seasonality(min_adm_date, today_date, MONTH_WEIGHTS)
    except Exception:
        admission_date = today_date

    los = draw_length_of_stay()
    proposed_discharge = admission_date + timedelta(days=los)

    if proposed_discharge > today_date:
        # Genuinely still mid-stay as of today — self-resolving,
        # not a permanent state
        discharge_date = None
        active_patients[patient_id] = True
    else:
        discharge_date = proposed_discharge
        last_discharge_dict[patient_id] = discharge_date

    is_readmission = "Y" if patient_id in last_discharge_dict and last_discharge_dict.get(patient_id) is not None else "N"

    raw_events.append({
        "patient_id": patient_id,
        "admission_date": admission_date,
        "discharge_date": discharge_date,
        "is_readmission": is_readmission,
    })

still_active_count = sum(1 for e in raw_events if e["discharge_date"] is None)
print(f"✅ Generated {len(raw_events):,} admission date-ranges after {attempts:,} attempts.")
print(f"ℹ️  Still active as of today: {still_active_count:,} (was ~2,400 under the old logic — should be well under 392 total system capacity now)")

# ============================================================
# CELL 4 — PHASE 2: Capacity-aware ward assignment
# Processes admissions in chronological order (by admission_date),
# tracking active (undischarged) intervals per ward, and only
# assigns a ward if it has capacity for the interval's duration.
# ============================================================
raw_events_sorted = sorted(raw_events, key=lambda e: e["admission_date"])

# ward -> list of (admission_date, discharge_date_or_none) still "active"
active_intervals = {w: [] for w in WARD_KEYS}

def prune_expired(ward, as_of_date):
    """Remove intervals that have already ended before as_of_date."""
    active_intervals[ward] = [
        iv for iv in active_intervals[ward]
        if iv[1] is None or iv[1] >= as_of_date
    ]

def assign_ward(admission_date, discharge_date):
    """Pick a ward with available capacity for this admission's stay.
    Falls back to the least-over-capacity ward only if every ward is full
    (mirrors real hospitals occasionally running over capacity in a crunch,
    rather than silently allowing unlimited overcapacity everywhere)."""
    candidates = []
    for ward in WARD_KEYS:
        prune_expired(ward, admission_date)
        current_count = len(active_intervals[ward])
        capacity = WARD_CAPACITIES[ward]
        if current_count < capacity:
            candidates.append((ward, capacity - current_count))

    if candidates:
        # Weight toward wards with more free capacity, keeps distribution natural
        wards, free_capacity = zip(*candidates)
        chosen_ward = random.choices(wards, weights=free_capacity, k=1)[0]
    else:
        # Every ward full — pick the one with the smallest overage
        chosen_ward = min(WARD_KEYS, key=lambda w: len(active_intervals[w]) - WARD_CAPACITIES[w])

    active_intervals[chosen_ward].append((admission_date, discharge_date))
    return chosen_ward

for event in raw_events_sorted:
    event["admitting_ward"] = assign_ward(event["admission_date"], event["discharge_date"])

print("✅ Capacity-aware ward assignment complete.")

# Sanity check: report any ward that ever exceeded capacity, and by how much
max_overage = {}
for ward in WARD_KEYS:
    max_concurrent = 0
    events_for_ward = [e for e in raw_events_sorted if e["admitting_ward"] == ward]
    for e in events_for_ward:
        prune_expired(ward, e["admission_date"])
    # Recompute peak concurrency properly via sweep
    intervals = [(e["admission_date"], e["discharge_date"] or today_date) for e in events_for_ward]
    events_ts = sorted([(s, 1) for s, e in intervals] + [(e, -1) for s, e in intervals])
    running, peak = 0, 0
    for _, delta in events_ts:
        running += delta
        peak = max(peak, running)
    max_overage[ward] = (peak, WARD_CAPACITIES[ward])

print("\nPeak concurrent occupancy vs. capacity, by ward:")
for ward, (peak, cap) in max_overage.items():
    flag = "⚠️ OVER" if peak > cap else "✅"
    print(f"  {ward:35s} peak={peak:4d}  capacity={cap:4d}  {flag}")


# ============================================================
# CELL 5 — PHASE 3: Fill in remaining fields & write to Bronze
# ============================================================
admission_schema = StructType([
    StructField("admission_id",          StringType(),  False),
    StructField("patient_id",            StringType(),  False),
    StructField("visit_number",          StringType(),  True),
    StructField("admission_date",        DateType(),    True),
    StructField("admission_time",        StringType(),  True),
    StructField("admission_type",        StringType(),  True),
    StructField("referring_source",      StringType(),  True),
    StructField("admitting_ward",        StringType(),  True),
    StructField("admitting_doctor_id",   StringType(),  True),
    StructField("facility_name",         StringType(),  True),
    StructField("discharge_date",        DateType(),    True),
    StructField("discharge_time",        StringType(),  True),
    StructField("discharge_ward",        StringType(),  True),
    StructField("discharge_disposition", StringType(),  True),
    StructField("discharge_doctor_id",   StringType(),  True),
    StructField("admission_status",      StringType(),  True),
    StructField("diagnosis",             StringType(),  True),
    StructField("length_of_stay_days",   IntegerType(), True),
    StructField("is_readmission",        StringType(),  True),
    StructField("icu_flag",              StringType(),  True),
    StructField("source_system",         StringType(),  True),
    StructField("ingestion_date",        DateType(),    True),
    StructField("ingestion_timestamp",   TimestampType(),True),
    StructField("record_hash",           StringType(),  True),
    StructField("is_duplicate_flag",     StringType(),  True),
    StructField("data_quality_flag",     StringType(),  True),
])

data = []
for e in raw_events_sorted:
    patient_id      = e["patient_id"]
    admission_date  = e["admission_date"]
    discharge_date  = e["discharge_date"]
    admitting_ward  = e["admitting_ward"]

    if discharge_date:
        disposition    = random.choices(DISCHARGE_DISPOSITIONS, weights=DISCHARGE_WEIGHTS)[0]
        discharge_ward = random.choice(WARD_KEYS)
        discharge_hour = random.randint(0, 23)
        discharge_min  = random.choice([0, 15, 30, 45])
        discharge_time = f"{discharge_hour:02d}:{discharge_min:02d}"
        status         = "Deceased" if disposition == "Deceased" else "Discharged"
        discharge_dr   = random.choice(DOCTOR_IDS)
        los            = (discharge_date - admission_date).days
    else:
        disposition, discharge_ward, discharge_time = None, None, None
        status, discharge_dr, los = "Admitted", None, None

    adm_hour, adm_min = random.randint(0, 23), random.choice([0, 15, 30, 45])
    visit_number = f"V-{random.randint(100000, 999999)}"
    record_hash  = hashlib.md5(f"{patient_id}{str(admission_date)}{visit_number}".encode()).hexdigest()

    data.append({
        "admission_id": str(uuid.uuid4()), "patient_id": patient_id,
        "visit_number": visit_number, "admission_date": admission_date,
        "admission_time": f"{adm_hour:02d}:{adm_min:02d}",
        "admission_type": random.choices(ADMISSION_TYPES, weights=ADMISSION_WEIGHTS)[0],
        "referring_source": random.choice(REFERRING_SOURCES),
        "admitting_ward": admitting_ward,
        "admitting_doctor_id": random.choice(DOCTOR_IDS),
        "facility_name": random.choice(FACILITIES),
        "discharge_date": discharge_date, "discharge_time": discharge_time,
        "discharge_ward": discharge_ward, "discharge_disposition": disposition,
        "discharge_doctor_id": discharge_dr, "admission_status": status,
        "diagnosis": patient_dict[patient_id], "length_of_stay_days": los,
        "is_readmission": e["is_readmission"],
        "icu_flag": "Y" if admitting_ward == "ICU" else random.choices(["Y","N"], weights=[0.10,0.90])[0],
        "source_system": random.choice(SOURCE_SYSTEMS),
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash, "is_duplicate_flag": "N", "data_quality_flag": "PASS",
    })

df_admissions = spark.createDataFrame(data, schema=admission_schema)

(
    df_admissions.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.01_bronze.admission_discharge")
)

print(f"✅ hospital_analytics.01_bronze.admission_discharge regenerated | {len(data):,} records | {ingestion_date}")

