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
# REFERENCE DATA 
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

WARD_KEYS          = [
    "Casualty", "ICU", "General Medical", "Surgical", "Paediatrics",
    "Maternity", "Oncology", "Orthopaedics", "Psychiatry", "Cardiology",
    "Isolation / Infectious Disease", "Outpatient"
]

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
# SCHEMA 
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

# ============================================================
# READ PATIENT IDs & DIAGNOSES FROM PATIENT_INFO BRONZE
# (Unity Catalog three-level name)
# ============================================================
patient_df   = spark.read.table("hospital_analytics.01_bronze.patient_info") \
                         .select("patient_id", "primary_diagnosis")
patient_data = patient_df.collect()
patient_dict = {row["patient_id"]: row["primary_diagnosis"] for row in patient_data}
patient_ids  = list(patient_dict.keys())

# ============================================================
# SIMULATE ADMISSIONS/DISCHARGES
# ============================================================
last_discharge_dict = {}
last_record         = {}
active_patients     = {}
monthly_counts      = {}

target_records = 8_000
today_date     = date.today()
ingestion_ts   = datetime.now()
ingestion_date = ingestion_ts.date()
data           = []
attempts       = 0

while len(data) < target_records:
    attempts += 1
    patient_id = random.choice(patient_ids)
    diagnosis  = patient_dict[patient_id]

    if patient_id in active_patients:
        continue
    if last_record.get(patient_id) == (today_date, today_date):
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

    adm_key = (admission_date.year, admission_date.month)
    cap     = MONTHLY_CAPS.get(admission_date.month, target_records // 6)
    if monthly_counts.get(adm_key, 0) >= cap:
        continue
    monthly_counts[adm_key] = monthly_counts.get(adm_key, 0) + 1

    if fake.boolean(chance_of_getting_true=70):
        los                = random.randint(1, 14)
        proposed_discharge = admission_date + timedelta(days=los)
        if proposed_discharge > today_date:
            discharge_date = None
            los            = None
            active_patients[patient_id] = True
        else:
            discharge_date = proposed_discharge
            last_discharge_dict[patient_id] = discharge_date
    else:
        discharge_date = None
        los            = None
        active_patients[patient_id] = True
        last_discharge_dict[patient_id] = None

    if discharge_date:
        disposition    = random.choices(DISCHARGE_DISPOSITIONS, weights=DISCHARGE_WEIGHTS)[0]
        discharge_ward = random.choice(WARD_KEYS)
        discharge_hour = random.randint(0, 23)
        discharge_min  = random.choice([0, 15, 30, 45])
        discharge_time = f"{discharge_hour:02d}:{discharge_min:02d}"
        status         = "Deceased" if disposition == "Deceased" else "Discharged"
        discharge_dr   = random.choice(DOCTOR_IDS)
    else:
        disposition    = None
        discharge_ward = None
        discharge_time = None
        status         = "Admitted"
        discharge_dr   = None

    adm_hour = random.randint(0, 23)
    adm_min  = random.choice([0, 15, 30, 45])

    is_readmission = "Y" if patient_id in last_discharge_dict and \
                            last_discharge_dict.get(patient_id) is not None else "N"

    admitting_ward = random.choice(WARD_KEYS)
    visit_number = f"V-{random.randint(100000, 999999)}"
    record_hash  = hashlib.md5(
        f"{patient_id}{str(admission_date)}{visit_number}".encode()
    ).hexdigest()

    last_record[patient_id] = (admission_date, discharge_date)

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
        "diagnosis": diagnosis, "length_of_stay_days": los,
        "is_readmission": is_readmission,
        "icu_flag": "Y" if admitting_ward == "ICU" else
                    random.choices(["Y","N"], weights=[0.10, 0.90])[0],
        "source_system": random.choice(SOURCE_SYSTEMS),
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash, "is_duplicate_flag": "N", "data_quality_flag": "PASS",
    })

print(f"✅ Generated {len(data):,} records after {attempts:,} attempts.")

# ============================================================
# CREATE DATAFRAME & WRITE TO UNITY CATALOG
# ============================================================
df_admissions = spark.createDataFrame(data, schema=admission_schema)

(
    df_admissions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ingestion_date")
    .saveAsTable("hospital_analytics.01_bronze.admission_discharge")
)

print(f"✅ hospital_analytics.01_bronze.admission_discharge written | {ingestion_date}")
