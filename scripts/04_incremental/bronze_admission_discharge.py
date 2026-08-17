# ============================================================
# SETUP
# ============================================================
%pip install faker

from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType
from delta.tables import DeltaTable
from faker import Faker
from datetime import datetime, date, timedelta
import random
import uuid
import hashlib

fake = Faker("en_GB")
Faker.seed(None)
random.seed(None)

today_date     = date.today()
ingestion_ts   = datetime.now()
ingestion_date = ingestion_ts.date()

# ============================================================
# REFERENCE DATA (matches original Bronze generator)
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
DISCHARGE_WEIGHTS = [0.60, 0.12, 0.05, 0.04, 0.15, 0.04]
SOURCE_SYSTEMS    = ["MedTech EMR", "GoodX", "Healthware", "Nexus EMR", "Paper-Digitised"]
WARD_KEYS = [
    "Casualty", "ICU", "General Medical", "Surgical", "Paediatrics",
    "Maternity", "Oncology", "Orthopaedics", "Psychiatry", "Cardiology",
    "Isolation / Infectious Disease", "Outpatient"
]

bronze_admission_table = "hospital_analytics.01_bronze.admission_discharge"
bronze_patient_table   = "hospital_analytics.01_bronze.patient_info"

# Sample from the fixed doctor roster (not random per-record — matches the earlier fix)
doctor_roster_df = spark.read.table("hospital_analytics.03_gold.dim_doctor").select("doctor_id")
DOCTOR_IDS = [row["doctor_id"] for row in doctor_roster_df.collect()]

# Patient pool + their diagnosis, for new admissions
patient_df   = spark.read.table(bronze_patient_table).select("patient_id", "primary_diagnosis")
patient_dict = {row["patient_id"]: row["primary_diagnosis"] for row in patient_df.collect()}
patient_ids  = list(patient_dict.keys())

# ============================================================
# PART 1: DISCHARGE A PORTION OF CURRENTLY ACTIVE ADMISSIONS
# (MERGE update — this is the CDC-style lifecycle change)
# ============================================================
delta_bronze = DeltaTable.forName(spark, bronze_admission_table)

active_df = (
    spark.read.format("delta").table(bronze_admission_table)
    .filter(col("discharge_date").isNull())
    .select("admission_id", "admission_date")
)
active_rows = active_df.collect()
print(f"📋 Currently active (undischarged) admissions: {len(active_rows):,}")

DISCHARGE_FRACTION = 0.30   # 30% of active admissions get discharged each run
sample_size = int(len(active_rows) * DISCHARGE_FRACTION)
sampled = random.sample(active_rows, min(sample_size, len(active_rows))) if active_rows else []

discharge_updates = []
for row in sampled:
    admission_id   = row["admission_id"]
    admission_date = row["admission_date"]
    lower_bound    = admission_date + timedelta(days=1)
    if lower_bound > today_date:
        continue

    discharge_date = fake.date_between(start_date=lower_bound, end_date=today_date)
    disposition    = random.choices(DISCHARGE_DISPOSITIONS, weights=DISCHARGE_WEIGHTS)[0]
    discharge_ward = random.choice(WARD_KEYS)
    discharge_hour = random.randint(0, 23)
    discharge_min  = random.choice([0, 15, 30, 45])
    discharge_time = f"{discharge_hour:02d}:{discharge_min:02d}"
    status         = "Deceased" if disposition == "Deceased" else "Discharged"
    discharge_dr   = random.choice(DOCTOR_IDS)
    los            = (discharge_date - admission_date).days

    discharge_updates.append({
        "admission_id": admission_id,
        "discharge_date": discharge_date,
        "discharge_time": discharge_time,
        "discharge_ward": discharge_ward,
        "discharge_disposition": disposition,
        "discharge_doctor_id": discharge_dr,
        "admission_status": status,
        "length_of_stay_days": los,
        "ingestion_date": ingestion_date,
        "ingestion_timestamp": ingestion_ts,
    })

if discharge_updates:
    updates_df = spark.createDataFrame(discharge_updates)
    (
        delta_bronze.alias("t")
        .merge(updates_df.alias("s"), "t.admission_id = s.admission_id")
        .whenMatchedUpdate(set={
            "discharge_date":        "s.discharge_date",
            "discharge_time":        "s.discharge_time",
            "discharge_ward":        "s.discharge_ward",
            "discharge_disposition": "s.discharge_disposition",
            "discharge_doctor_id":   "s.discharge_doctor_id",
            "admission_status":      "s.admission_status",
            "length_of_stay_days":   "s.length_of_stay_days",
            "ingestion_date":        "s.ingestion_date",
            "ingestion_timestamp":   "s.ingestion_timestamp",
        })
        .execute()
    )
    print(f"✅ Discharged {len(discharge_updates):,} previously active admissions.")
else:
    print("ℹ️  No active admissions were discharged today.")

# ============================================================
# ADD to incremental_bronze_admission_discharge, before Cell 4
# (new admissions section) — capacity-aware ward assignment
# ============================================================
WARD_CAPACITIES = {
    "Casualty": 30, "ICU": 12, "General Medical": 60, "Surgical": 40,
    "Paediatrics": 35, "Maternity": 40, "Oncology": 25, "Orthopaedics": 30,
    "Psychiatry": 30, "Cardiology": 20, "Isolation / Infectious Disease": 20,
    "Outpatient": 50,
}

current_ward_counts_df = (
    spark.read.format("delta").table(bronze_admission_table)
    .filter(col("discharge_date").isNull())
    .groupBy("admitting_ward").count()
)
current_ward_counts = {row["admitting_ward"]: row["count"] for row in current_ward_counts_df.collect()}

def assign_ward_with_capacity():
    candidates = [
        (w, cap - current_ward_counts.get(w, 0))
        for w, cap in WARD_CAPACITIES.items()
        if current_ward_counts.get(w, 0) < cap
    ]
    if candidates:
        wards, free = zip(*candidates)
        chosen = random.choices(wards, weights=free, k=1)[0]
    else:
        chosen = min(WARD_CAPACITIES, key=lambda w: current_ward_counts.get(w, 0) - WARD_CAPACITIES[w])
    current_ward_counts[chosen] = current_ward_counts.get(chosen, 0) + 1  # reserve it for this batch
    return chosen

# ============================================================
# PART 2: NEW ADMISSIONS (append)
# ============================================================
NUM_NEW_ADMISSIONS = 300

# Track which patients currently have an active (undischarged) admission —
# a patient can't be admitted twice at once
currently_active_patients = set(
    row["patient_id"] for row in
    spark.read.format("delta").table(bronze_admission_table)
    .filter(col("discharge_date").isNull())
    .select("patient_id").distinct().collect()
)

# Track patients with any prior discharge, for readmission flagging
previously_discharged_patients = set(
    row["patient_id"] for row in
    spark.read.format("delta").table(bronze_admission_table)
    .filter(col("discharge_date").isNotNull())
    .select("patient_id").distinct().collect()
)

new_admissions   = []
eligible_patients = [pid for pid in patient_ids if pid not in currently_active_patients]

for _ in range(min(NUM_NEW_ADMISSIONS, len(eligible_patients))):
    patient_id = random.choice(eligible_patients)
    eligible_patients.remove(patient_id)   # avoid double-booking within this batch
    diagnosis  = patient_dict[patient_id]

    admission_date = today_date  # incremental loads land admissions "today"
    admission_hour = random.randint(0, 23)
    admission_min  = random.choice([0, 15, 30, 45])
    visit_number   = f"V-{random.randint(100000, 999999)}"
    admitting_ward = assign_ward_with_capacity()

    is_readmission = "Y" if patient_id in previously_discharged_patients else "N"

    record_hash = hashlib.md5(
        f"{patient_id}{str(admission_date)}{visit_number}".encode()
    ).hexdigest()

    new_admissions.append({
        "admission_id": str(uuid.uuid4()), "patient_id": patient_id,
        "visit_number": visit_number, "admission_date": admission_date,
        "admission_time": f"{admission_hour:02d}:{admission_min:02d}",
        "admission_type": random.choices(ADMISSION_TYPES, weights=ADMISSION_WEIGHTS)[0],
        "referring_source": random.choice(REFERRING_SOURCES),
        "admitting_ward": admitting_ward,
        "admitting_doctor_id": random.choice(DOCTOR_IDS),
        "facility_name": random.choice(FACILITIES),
        "discharge_date": None, "discharge_time": None,
        "discharge_ward": None, "discharge_disposition": None,
        "discharge_doctor_id": None, "admission_status": "Admitted",
        "diagnosis": diagnosis, "length_of_stay_days": None,
        "is_readmission": is_readmission,
        "icu_flag": "Y" if admitting_ward == "ICU" else
                    random.choices(["Y","N"], weights=[0.10, 0.90])[0],
        "source_system": random.choice(SOURCE_SYSTEMS),
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash, "is_duplicate_flag": "N", "data_quality_flag": "PASS",
    })

print(f"✅ Generated {len(new_admissions):,} new admissions.")

# ============================================================
# WRITE (APPEND) NEW ADMISSIONS TO BRONZE
# ============================================================
if new_admissions:
    # Use the Bronze table schema to handle nullable columns with all None values
    bronze_schema = spark.read.format("delta").table(bronze_admission_table).schema
    new_df = spark.createDataFrame(new_admissions, schema=bronze_schema)
    new_df.write.format("delta").mode("append").saveAsTable(bronze_admission_table)
    print(f"✅ Appended {len(new_admissions):,} new admissions | {ingestion_date}")
else:
    print("ℹ️  No new admissions generated today.")
  
