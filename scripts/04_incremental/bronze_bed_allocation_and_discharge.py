# ============================================================
# CELL 1 : SETUP & READ TODAY'S CHANGED ADMISSIONS
# ============================================================
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType
)
from delta.tables import DeltaTable
from datetime import datetime, date
import random
import uuid
import hashlib

today_date     = date.today()
ingestion_ts   = datetime.now()
ingestion_date = ingestion_ts.date()

bronze_admission_table = "hospital_analytics.01_bronze.admission_discharge"
bronze_bed_table       = "hospital_analytics.01_bronze.bed_allocation"

load_date = spark.sql("SELECT current_date() as current_date").collect()[0]["current_date"]

changed_admissions_df = (
    spark.read.format("delta").table(bronze_admission_table)
    .filter(col("ingestion_date") == lit(load_date))
    .select("admission_id", "patient_id", "admission_date", "discharge_date", "admitting_ward")
)
print(f"📥 Admissions changed today: {changed_admissions_df.count():,}")

# ============================================================
# CELL 2 : REFERENCE DATA (matches original bed_allocation Bronze)
# ============================================================
BED_TYPES = {
    "ICU": "ICU Bed", "Casualty": "High Care Bed", "Maternity": "Maternity Bed",
    "Paediatrics": "Paediatric Bed", "Isolation / Infectious Disease": "Isolation Bed",
    "General Medical": "General Ward Bed", "Surgical": "General Ward Bed",
    "Oncology": "General Ward Bed", "Orthopaedics": "General Ward Bed",
    "Psychiatry": "General Ward Bed", "Cardiology": "High Care Bed",
    "Outpatient": "Day Ward Bed",
}
WARD_CAPACITIES = {
    "Casualty": 30, "ICU": 12, "General Medical": 60, "Surgical": 40,
    "Paediatrics": 35, "Maternity": 40, "Oncology": 25, "Orthopaedics": 30,
    "Psychiatry": 30, "Cardiology": 20, "Isolation / Infectious Disease": 20,
    "Outpatient": 50,
}
SOURCE_SYSTEMS = ["MedTech EMR", "GoodX", "Healthware", "Nexus EMR", "Paper-Digitised"]

# ============================================================
# CELL 3 : PART 1: RELEASE BEDS FOR NEWLY DISCHARGED ADMISSIONS
# (MERGE update)
# ============================================================
delta_bed = DeltaTable.forName(spark, bronze_bed_table)

newly_discharged = changed_admissions_df.filter(col("discharge_date").isNotNull()).collect()
print(f"🛏️  Newly discharged admissions to release beds for: {len(newly_discharged):,}")

bed_release_updates = []
for row in newly_discharged:
    bed_release_updates.append({
        "admission_id": row["admission_id"],
        "discharge_date": row["discharge_date"],
        "bed_status": "Available",
        "ingestion_date": ingestion_date,
        "ingestion_timestamp": ingestion_ts,
    })

if bed_release_updates:
    release_schema = StructType([
        StructField("admission_id",        StringType(),    True),
        StructField("discharge_date",      DateType(),      True),
        StructField("bed_status",          StringType(),    True),
        StructField("ingestion_date",      DateType(),      True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])
    releases_df = spark.createDataFrame(bed_release_updates, schema=release_schema)

    (
        delta_bed.alias("t")
        .merge(releases_df.alias("s"), "t.admission_id = s.admission_id")
        .whenMatchedUpdate(set={
            "discharge_date":      "s.discharge_date",
            "bed_status":          "s.bed_status",
            "ingestion_date":      "s.ingestion_date",
            "ingestion_timestamp": "s.ingestion_timestamp",
        })
        .execute()
    )
    print(f"✅ Released {len(bed_release_updates):,} beds.")
else:
    print("ℹ️  No beds to release today.")

# ============================================================
# CELL 4 : PART 2: NEW BED ALLOCATIONS FOR NEW ADMISSIONS (append)
# ============================================================
new_admissions = changed_admissions_df.filter(col("discharge_date").isNull()).collect()
print(f"🛏️  New admissions needing a bed: {len(new_admissions):,}")

bed_schema = StructType([
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

new_bed_records = []
for row in new_admissions:
    ward_key = row["admitting_ward"] if row["admitting_ward"] else random.choice(list(WARD_CAPACITIES.keys()))
    bed_type = BED_TYPES.get(ward_key, "General Ward Bed")
    prefix   = ward_key[:3].upper().replace(" ", "")
    capacity = WARD_CAPACITIES.get(ward_key, 30)
    bed_number = f"{prefix}-{random.randint(1, capacity):02d}"
    bed_id     = f"BED-{ward_key[:3].upper()}-{random.randint(1000, 9999)}"

    record_hash = hashlib.md5(
        f"{row['patient_id']}{row['admission_id']}{str(row['admission_date'])}{bed_id}".encode()
    ).hexdigest()

    new_bed_records.append({
        "bed_allocation_id": str(uuid.uuid4()), "admission_id": row["admission_id"],
        "patient_id": row["patient_id"], "ward_key": ward_key, "bed_id": bed_id,
        "bed_type": bed_type, "bed_number": bed_number,
        "admission_date": row["admission_date"], "discharge_date": None,
        "length_of_stay_days": None, "bed_status": "Occupied",
        "source_system": random.choice(SOURCE_SYSTEMS),
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash, "is_duplicate_flag": "N", "data_quality_flag": "PASS",
    })

if new_bed_records:
    new_bed_df = spark.createDataFrame(new_bed_records, schema=bed_schema)
    new_bed_df.write.format("delta").mode("append").saveAsTable(bronze_bed_table)
    print(f"✅ Appended {len(new_bed_records):,} new bed allocations | {ingestion_date}")
else:
    print("ℹ️  No new bed allocations today.")

