# ============================================================
# CELL 1 : SETUP
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

fake = Faker("en_GB")
Faker.seed(42)
random.seed(42)

print("Setup complete.")

# ============================================================
# CELL 2 : RESOURCE TYPES & REFERENCE DATA
# ============================================================
RESOURCE_TYPES = [
    # Beds & Accommodation
    "General Ward Bed", "ICU Bed", "High Care Bed", "Maternity Bed",
    "Paediatric Bed", "Isolation Bed", "Day Ward Bed",
    # Medical Equipment
    "Ventilator", "ECG Machine", "Ultrasound Machine", "X-Ray Machine",
    "MRI Scanner", "CT Scanner", "Defibrillator", "Infusion Pump",
    "Dialysis Machine", "Pulse Oximeter", "Blood Glucose Monitor",
    "Anaesthesia Machine", "Surgical Lamp", "Endoscope",
    # Staff
    "Specialist Doctor", "Medical Officer", "Intern Doctor", "Registered Nurse",
    "Enrolled Nurse", "Scrub Nurse", "ICU Nurse", "Midwife", "Paramedic",
    "Radiographer", "Pharmacist", "Physiotherapist", "Occupational Therapist",
    "Social Worker", "Dietician",
    # Theatre & Surgical
    "Operating Theatre", "Surgical Instrument Set", "Laparoscopic Equipment",
    "Orthopaedic Drill Set",
    # Pharmacy & Consumables
    "Blood Unit (O+)", "Blood Unit (A+)", "Blood Unit (B+)", "Blood Unit (AB+)",
    "IV Fluid Stock", "PPE Stock", "Sterile Dressing Pack",
    # Support & Logistics
    "Ambulance", "Patient Transport Wheelchair", "Patient Transport Stretcher",
    "Mortuary Bay",
]

RESOURCE_WEIGHTS = (
    [3.0] * 7  +   # Beds
    [2.0] * 14 +   # Equipment
    [4.0] * 15 +   # Staff
    [1.5] * 4  +   # Theatre
    [1.0] * 7  +   # Pharmacy
    [1.0] * 4      # Support
)

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

# ============================================================
# CELL 3 : SCHEMA (add facility_name field)
# ============================================================
schema = StructType([
    StructField("resource_id",            StringType(), False),
    StructField("resource_type",          StringType(), True),
    StructField("facility_name",          StringType(), True),   # ← new
    StructField("ward_key",               StringType(), True),
    StructField("patient_id",             StringType(), True),
    StructField("assignment_start_date",  DateType(),   True),
    StructField("assignment_end_date",    DateType(),   True),
    StructField("resource_status",        StringType(), True),
    StructField("ingestion_date",         DateType(),   True),
    StructField("ingestion_timestamp",    TimestampType(), True),
    StructField("record_hash",            StringType(), True),
])

patient_df  = spark.read.table("hospital_analytics.01_bronze.patient_info").select("patient_id")
patient_ids = [row["patient_id"] for row in patient_df.collect()]

# ============================================================
# CELL 4 — GENERATE RECORDS (add facility_name to each record)
# ============================================================
num_records    = 200_000
today_date     = date.today()
ingestion_ts   = datetime.now()
ingestion_date = ingestion_ts.date()
data           = []

for _ in range(num_records):

    resource_id   = str(uuid.uuid4())
    resource_type = random.choices(RESOURCE_TYPES, weights=RESOURCE_WEIGHTS)[0]
    facility_name = random.choice(FACILITIES)          # ← new
    ward_key      = random.choice(WARD_KEYS)
    patient_id    = random.choice(patient_ids)

    assignment_start_date = fake.date_between(start_date="-4y", end_date="today")

    if fake.boolean(chance_of_getting_true=50):
        lower_bound = assignment_start_date + timedelta(days=1)
        if lower_bound > today_date:
            assignment_end_date = today_date
        else:
            try:
                assignment_end_date = fake.date_between(
                    start_date=lower_bound, end_date=today_date
                )
            except Exception:
                assignment_end_date = today_date
    else:
        assignment_end_date = None

    resource_status = "Available" if assignment_end_date else "Assigned"

    record_hash = hashlib.md5(
        f"{resource_id}{patient_id}{str(assignment_start_date)}".encode()
    ).hexdigest()

    data.append({
        "resource_id": resource_id,
        "resource_type": resource_type,
        "facility_name": facility_name,     # ← new
        "ward_key": ward_key,
        "patient_id": patient_id,
        "assignment_start_date": assignment_start_date,
        "assignment_end_date": assignment_end_date,
        "resource_status": resource_status,
        "ingestion_date": ingestion_date,
        "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash,
    })

print(f"✅ Generated {len(data):,} records.")

# ============================================================
# CELL 5 : CREATE DATAFRAME & WRITE TO UNITY CATALOG
# ============================================================
df = spark.createDataFrame(data, schema=schema)

table_name = "hospital_analytics.01_bronze.resource_allocation"
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ingestion_date")
    .saveAsTable(table_name)
)

print(f"✅ {table_name} created successfully | {num_records:,} records | {ingestion_date}")
