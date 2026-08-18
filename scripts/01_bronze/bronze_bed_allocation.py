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
# CELL 2 : REFERENCE DATA 
# ============================================================
WARD_KEYS = [
    "Casualty", "ICU", "General Medical", "Surgical", "Paediatrics",
    "Maternity", "Oncology", "Orthopaedics", "Psychiatry", "Cardiology",
    "Isolation / Infectious Disease", "Outpatient"
]

BED_TYPES = {
    "ICU":                           "ICU Bed",
    "Casualty":                      "High Care Bed",
    "Maternity":                     "Maternity Bed",
    "Paediatrics":                   "Paediatric Bed",
    "Isolation / Infectious Disease":"Isolation Bed",
    "General Medical":               "General Ward Bed",
    "Surgical":                      "General Ward Bed",
    "Oncology":                      "General Ward Bed",
    "Orthopaedics":                  "General Ward Bed",
    "Psychiatry":                    "General Ward Bed",
    "Cardiology":                    "High Care Bed",
    "Outpatient":                    "Day Ward Bed",
}

WARD_CAPACITIES = {
    "Casualty": 30, "ICU": 12, "General Medical": 60, "Surgical": 40,
    "Paediatrics": 35, "Maternity": 40, "Oncology": 25, "Orthopaedics": 30,
    "Psychiatry": 30, "Cardiology": 20, "Isolation / Infectious Disease": 20,
    "Outpatient": 50,
}

SOURCE_SYSTEMS = ["MedTech EMR", "GoodX", "Healthware", "Nexus EMR", "Paper-Digitised"]

# ============================================================
# CELL 3 : SCHEMA
# ============================================================
schema = StructType([
    StructField("bed_allocation_id",   StringType(),    False),
    StructField("admission_id",        StringType(),    True),
    StructField("patient_id",          StringType(),    True),
    StructField("ward_key",            StringType(),    True),
    StructField("bed_id",              StringType(),    True),
    StructField("bed_type",            StringType(),    True),
    StructField("bed_number",          StringType(),    True),
    StructField("admission_date",      DateType(),      True),
    StructField("discharge_date",      DateType(),      True),
    StructField("length_of_stay_days", IntegerType(),   True),
    StructField("bed_status",          StringType(),    True),
    StructField("source_system",       StringType(),    True),
    StructField("ingestion_date",      DateType(),      True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("record_hash",         StringType(),    True),
    StructField("is_duplicate_flag",   StringType(),    True),
    StructField("data_quality_flag",   StringType(),    True),
])

# ============================================================
# CELL 4 : hospital_analytics.bronze.patient_infohospital_analytics.bronze.patient_info
# READ ADMISSIONS, GENERATE BED RECORDS, WRITE
# ============================================================
adm_df = spark.read.format("delta").table("hospital_analytics.01_bronze.admission_discharge")
adm_data = adm_df.select(
    "admission_id", "patient_id", "admission_date",
    "discharge_date", "admitting_ward"
).collect()

adm_list = []
for row in adm_data:
    adm_list.append({
        "admission_id": row["admission_id"],
        "patient_id":   row["patient_id"],
        "adm_date":     row["admission_date"],
        "dis_date":     row["discharge_date"],
        "ward_key":     row["admitting_ward"],
    })

today_date     = date.today()
ingestion_ts   = datetime.now()
ingestion_date = ingestion_ts.date()
bed_records    = []

for adm_record in adm_list:
    patient_id       = adm_record["patient_id"]
    admission_id     = adm_record["admission_id"]
    patient_adm_date = adm_record["adm_date"]
    patient_dis_date = adm_record["dis_date"]

    ward_key = adm_record["ward_key"] if adm_record["ward_key"] else random.choice(WARD_KEYS)
    bed_type = BED_TYPES.get(ward_key, "General Ward Bed")

    prefix     = ward_key[:3].upper().replace(" ", "")
    capacity   = WARD_CAPACITIES.get(ward_key, 30)
    bed_number = f"{prefix}-{random.randint(1, capacity):02d}"
    bed_id     = f"BED-{ward_key[:3].upper()}-{random.randint(1000, 9999)}"

    upper_bound = (
        patient_dis_date
        if (patient_dis_date is not None and patient_dis_date < today_date)
        else today_date
    )

    if patient_adm_date >= upper_bound:
        bed_adm_date = patient_adm_date
    else:
        try:
            bed_adm_date = fake.date_between(start_date=patient_adm_date, end_date=upper_bound)
        except Exception:
            bed_adm_date = patient_adm_date

    if patient_dis_date is not None:
        lower_bound = bed_adm_date + timedelta(days=1)
        if lower_bound > patient_dis_date:
            bed_dis_date = patient_dis_date
        else:
            try:
                bed_dis_date = fake.date_between(start_date=lower_bound, end_date=patient_dis_date)
            except Exception:
                bed_dis_date = patient_dis_date
    else:
        if fake.boolean(chance_of_getting_true=50):
            lower_bound = bed_adm_date + timedelta(days=1)
            try:
                bed_dis_date = fake.date_between(start_date=lower_bound, end_date=today_date)
            except Exception:
                bed_dis_date = today_date
        else:
            bed_dis_date = None

    los = (bed_dis_date - bed_adm_date).days if bed_dis_date else (today_date - bed_adm_date).days

    bed_status = "Available" if bed_dis_date is not None else "Occupied"

    if patient_adm_date is None:
        dq_flag = "FAIL - Missing Admission Date"
    elif bed_dis_date is not None and bed_dis_date < bed_adm_date:
        dq_flag = "FAIL - Discharge Before Admission"
    elif los > 365:
        dq_flag = "WARN - LOS Exceeds 1 Year"
    else:
        dq_flag = "PASS"

    bed_allocation_id = str(uuid.uuid4())
    record_hash = hashlib.md5(
        f"{patient_id}{admission_id}{str(bed_adm_date)}{bed_id}".encode()
    ).hexdigest()

    bed_records.append({
        "bed_allocation_id": bed_allocation_id, "admission_id": admission_id,
        "patient_id": patient_id, "ward_key": ward_key, "bed_id": bed_id,
        "bed_type": bed_type, "bed_number": bed_number,
        "admission_date": bed_adm_date, "discharge_date": bed_dis_date,
        "length_of_stay_days": los, "bed_status": bed_status,
        "source_system": random.choice(SOURCE_SYSTEMS),
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash, "is_duplicate_flag": "N", "data_quality_flag": dq_flag,
    })

bed_df = spark.createDataFrame(bed_records, schema=schema)

table_name = "hospital_analytics.01_bronze.bed_allocation"
(
    bed_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ingestion_date")
    .saveAsTable(table_name)
)

print(f"✅ {table_name} created successfully | {len(bed_records):,} records | {ingestion_date}")
