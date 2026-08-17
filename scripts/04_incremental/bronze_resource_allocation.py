# ============================================================
# incremental_resource_allocation_bronze
# ============================================================
%pip install faker

from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType
)
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

bronze_res_table     = "hospital_analytics.01_bronze.resource_allocation"
bronze_patient_table = "hospital_analytics.01_bronze.patient_info"

RESOURCE_TYPES = [
    "General Ward Bed", "ICU Bed", "High Care Bed", "Maternity Bed",
    "Paediatric Bed", "Isolation Bed", "Day Ward Bed",
    "Ventilator", "ECG Machine", "Ultrasound Machine", "X-Ray Machine",
    "MRI Scanner", "CT Scanner", "Defibrillator", "Infusion Pump",
    "Dialysis Machine", "Pulse Oximeter", "Blood Glucose Monitor",
    "Anaesthesia Machine", "Surgical Lamp", "Endoscope",
    "Specialist Doctor", "Medical Officer", "Intern Doctor", "Registered Nurse",
    "Enrolled Nurse", "Scrub Nurse", "ICU Nurse", "Midwife", "Paramedic",
    "Radiographer", "Pharmacist", "Physiotherapist", "Occupational Therapist",
    "Social Worker", "Dietician",
    "Operating Theatre", "Surgical Instrument Set", "Laparoscopic Equipment",
    "Orthopaedic Drill Set",
    "Blood Unit (O+)", "Blood Unit (A+)", "Blood Unit (B+)", "Blood Unit (AB+)",
    "IV Fluid Stock", "PPE Stock", "Sterile Dressing Pack",
    "Ambulance", "Patient Transport Wheelchair", "Patient Transport Stretcher",
    "Mortuary Bay",
]
RESOURCE_WEIGHTS = ([3.0]*7 + [2.0]*14 + [4.0]*15 + [1.5]*4 + [1.0]*7 + [1.0]*4)
WARD_KEYS = [
    "Casualty", "ICU", "General Medical", "Surgical", "Paediatrics",
    "Maternity", "Oncology", "Orthopaedics", "Psychiatry", "Cardiology",
    "Isolation / Infectious Disease", "Outpatient"
]
FACILITIES = [
    "Steve Biko Academic Hospital", "Kalafong Provincial Tertiary Hospital",
    "Tshwane District Hospital", "Unitas Hospital",
    "Little Company of Mary Hospital", "Netcare Montana Hospital", "Mediclinic Kloof",
]

patient_ids = [row["patient_id"] for row in
               spark.read.table(bronze_patient_table).select("patient_id").collect()]

# ============================================================
# PART 1: RELEASE A PORTION OF CURRENTLY ASSIGNED RESOURCES
# (MERGE update)
# ============================================================
delta_res = DeltaTable.forName(spark, bronze_res_table)

assigned_df = (
    spark.read.format("delta").table(bronze_res_table)
    .filter(col("assignment_end_date").isNull())
    .select("resource_id", "assignment_start_date")
)
assigned_rows = assigned_df.collect()
print(f"📋 Currently assigned resources: {len(assigned_rows):,}")

RELEASE_FRACTION = 0.30
sample_size = int(len(assigned_rows) * RELEASE_FRACTION)
sampled = random.sample(assigned_rows, min(sample_size, len(assigned_rows))) if assigned_rows else []

release_updates = []
for row in sampled:
    lower_bound = row["assignment_start_date"] + timedelta(days=1)
    if lower_bound > today_date:
        continue
    end_date = fake.date_between(start_date=lower_bound, end_date=today_date)
    release_updates.append({
        "resource_id": row["resource_id"],
        "assignment_end_date": end_date,
        "resource_status": "Available",
        "ingestion_date": ingestion_date,
        "ingestion_timestamp": ingestion_ts,
    })

if release_updates:
    release_schema = StructType([
        StructField("resource_id",          StringType(),    True),
        StructField("assignment_end_date",  DateType(),      True),
        StructField("resource_status",      StringType(),    True),
        StructField("ingestion_date",       DateType(),      True),
        StructField("ingestion_timestamp",  TimestampType(), True),
    ])
    releases_df = spark.createDataFrame(release_updates, schema=release_schema)

    (
        delta_res.alias("t")
        .merge(releases_df.alias("s"), "t.resource_id = s.resource_id")
        .whenMatchedUpdate(set={
            "assignment_end_date": "s.assignment_end_date",
            "resource_status":     "s.resource_status",
            "ingestion_date":      "s.ingestion_date",
            "ingestion_timestamp": "s.ingestion_timestamp",
        })
        .execute()
    )
    print(f"✅ Released {len(release_updates):,} resources.")
else:
    print("ℹ️  No resources released today.")

# ============================================================
# PART 2: NEW RESOURCE ASSIGNMENTS (append)
# ============================================================
NUM_NEW_ASSIGNMENTS = 400

res_schema = StructType([
    StructField("resource_id",            StringType(), False),
    StructField("resource_type",          StringType(), True),
    StructField("facility_name",          StringType(), True),
    StructField("ward_key",               StringType(), True),
    StructField("patient_id",             StringType(), True),
    StructField("assignment_start_date",  DateType(),   True),
    StructField("assignment_end_date",    DateType(),   True),
    StructField("resource_status",        StringType(), True),
    StructField("ingestion_date",         DateType(),   True),
    StructField("ingestion_timestamp",    TimestampType(), True),
    StructField("record_hash",            StringType(), True),
])

new_assignments = []
for _ in range(NUM_NEW_ASSIGNMENTS):
    resource_id   = str(uuid.uuid4())
    resource_type = random.choices(RESOURCE_TYPES, weights=RESOURCE_WEIGHTS)[0]
    facility_name = random.choice(FACILITIES)
    ward_key      = random.choice(WARD_KEYS)
    patient_id    = random.choice(patient_ids)

    record_hash = hashlib.md5(
        f"{resource_id}{patient_id}{str(today_date)}".encode()
    ).hexdigest()

    new_assignments.append({
        "resource_id": resource_id, "resource_type": resource_type,
        "facility_name": facility_name, "ward_key": ward_key, "patient_id": patient_id,
        "assignment_start_date": today_date, "assignment_end_date": None,
        "resource_status": "Assigned",
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash,
    })

new_df = spark.createDataFrame(new_assignments, schema=res_schema)
new_df.write.format("delta").mode("append").saveAsTable(bronze_res_table)
print(f"✅ Appended {len(new_assignments):,} new resource assignments | {ingestion_date}")

