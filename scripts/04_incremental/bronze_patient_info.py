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

fake = Faker("en_GB")
Faker.seed(None)     # intentionally non-deterministic — new patients each run
random.seed(None)

# ============================================================
# REFERENCE DATA & HELPERS
# ============================================================
MALE_NAMES = ["Sipho","Thabo","Kagiso","Lerato","Tebogo","Bongani","Nkosinathi",
              "Siyanda","Mpho","Lwazi","Johan","Pieter","Heinrich","Rohan",
              "Aryan","Devon","Brandon","Neo","Tshepo","Thomas","Esau",
              "Thulani","Werner","Thabiso","Sudeshen","Alfred","Lehlogonlo",
              "Collen","Obed","Jabulani","Moses","Peet","Jan","Thato"]
FEMALE_NAMES = ["Nomvula","Zanele","Thandiwe","Nompumelelo","Lerato","Ofentse","Boitumelo",
                "Kelebogile","Noluthando","Ayanda","Thembi","Annelie","Marietjie",
                "Fatima","Nadia","Candice","Cindy","Simone","Leani","Keamogetswe",
                "Palesa","Kamogelo","Tshegofatso","Oreneilwe","Georginah","Asanda",
                "Thuli","Thokozile","Venessa","Cathy","Keletso","Khomotso","Keabetswe"]
SA_SURNAMES = ["Dlamini","Nkosi","Mokoena","Mahlangu","Ndlovu","Mthembu","Zulu",
               "Sithole","Molefe","Mabunda","Sibiya","Van der Merwe","Botha","Pretorius",
               "Du Plessis","Joubert","Naidoo","Pillay","Reddy","Adams","Hendricks",
               "September","Isaacs","Khumalo","Shabalala","Mkhize","Cele","Ntuli",
               "Swanepoel","Khoza","Solomons","Swart","Pieterson","Lombardt","Sekhosana"]

PAEDIATRIC_DIAGNOSES = ["Pneumonia","Acute Gastroenteritis","Asthma",
                         "Neonatal Jaundice","Anaemia","Sepsis","Malaria"]
ADULT_DIAGNOSES = ["Hypertension","Type 2 Diabetes Mellitus","HIV/AIDS",
                    "Tuberculosis (TB)","Coronary Artery Disease","Stroke / CVA",
                    "Chronic Kidney Disease","Mental Health - Depression",
                    "Mental Health - Schizophrenia","Trauma - MVA","Trauma - Assault",
                    "Obstetric Complication","Appendicitis","Sepsis","Malaria",
                    "Pneumonia","Asthma","Anaemia","Acute Gastroenteritis"]

DEMOGRAPHICS    = ["Black African","White","Coloured","Indian/Asian","Other"]
DEMO_WEIGHTS    = [0.75, 0.15, 0.04, 0.04, 0.02]
PAYMENT_TYPES   = ["Medical Aid","Private (Self-pay)","Government (Public)","RAF","Uninsured"]
PAYMENT_WEIGHTS = [0.30, 0.10, 0.45, 0.05, 0.10]
MEDICAL_AIDS    = ["Discovery Health","GEMS","Bonitas","Momentum Health",
                    "Medihelp","Fedhealth","Bestmed","Hosmed"]
FACILITIES      = ["Steve Biko Academic Hospital","Kalafong Provincial Tertiary Hospital",
                    "Tshwane District Hospital","Unitas Hospital",
                    "Little Company of Mary Hospital","Netcare Montana Hospital",
                    "Mediclinic Kloof"]
SA_LANGUAGES    = ["Zulu","Xhosa","Afrikaans","English","Sepedi","Setswana",
                    "Sesotho","Tsonga","Venda","Ndebele","Swati"]
LANG_WEIGHTS    = [0.255, 0.198, 0.127, 0.086, 0.092, 0.081,
                    0.071, 0.033, 0.023, 0.019, 0.015]
PROVINCES       = ["GP","GP","GP","GP","GP","LP","MP","NW","FS"]
PRETORIA_AREAS  = ["Soshanguve","Mamelodi","Atteridgeville","Ga-Rankuwa","Centurion",
                    "Pretoria Central","Hatfield","Menlyn","Silverton","Mabopane"]
SOURCE_SYSTEMS  = ["MedTech EMR","GoodX","Healthware","Nexus EMR","Paper-Digitised"]

def get_marital_status(age: int) -> str:
    if age < 18:
        return "Single"
    elif age < 23:
        return random.choices(["Single", "Married"], weights=[0.90, 0.10])[0]
    elif age < 35:
        return random.choices(["Single", "Married", "Divorced"], weights=[0.50, 0.40, 0.10])[0]
    elif age < 60:
        return random.choices(["Single", "Married", "Divorced", "Widowed"], weights=[0.20, 0.55, 0.15, 0.10])[0]
    else:
        return random.choices(["Single", "Married", "Divorced", "Widowed"], weights=[0.10, 0.45, 0.15, 0.30])[0]

def get_diagnosis(age: int) -> str:
    return random.choice(PAEDIATRIC_DIAGNOSES) if age < 13 else random.choice(ADULT_DIAGNOSES)

def get_secondary_diagnosis(age: int, primary: str):
    if random.random() < 0.30:
        pool = PAEDIATRIC_DIAGNOSES if age < 13 else ADULT_DIAGNOSES
        options = [d for d in pool if d != primary]
        return random.choice(options) if options else None
    return None

# Add province column if missing (safe to re-run)
try:
    spark.sql("ALTER TABLE hospital_analytics.01_bronze.patient_info ADD COLUMN province STRING")
    print("✅ province column added to patient_info Bronze")
except Exception:
    print("ℹ️  province column already exists — skipping.")

# ============================================================
# BRONZE SCHEMA & READ EXISTING PATIENTS
# ============================================================
bronze_schema = StructType([
    StructField("patient_id",           StringType(),    False),
    StructField("id_number",            StringType(),    True),
    StructField("first_name",           StringType(),    True),
    StructField("last_name",            StringType(),    True),
    StructField("age",                  IntegerType(),   True),
    StructField("gender",               StringType(),    True),
    StructField("demographic",          StringType(),    True),
    StructField("marital_status",       StringType(),    True),
    StructField("preferred_language",   StringType(),    True),
    StructField("province",             StringType(),    True),
    StructField("city",                 StringType(),    True),
    StructField("postal_code",          StringType(),    True),
    StructField("primary_diagnosis",    StringType(),    True),
    StructField("secondary_diagnosis",  StringType(),    True),
    StructField("ward",                 StringType(),    True),
    StructField("admission_type",       StringType(),    True),
    StructField("last_visit_date",      DateType(),      True),
    StructField("discharge_date",       DateType(),      True),
    StructField("length_of_stay_days",  IntegerType(),   True),
    StructField("facility_name",        StringType(),    True),
    StructField("payment_type",         StringType(),    True),
    StructField("medical_aid_scheme",   StringType(),    True),
    StructField("attending_doctor_id",  StringType(),    True),
    StructField("source_system",        StringType(),    True),
    StructField("ingestion_date",       DateType(),      True),
    StructField("ingestion_timestamp",  TimestampType(), True),
    StructField("record_hash",          StringType(),    True),
    StructField("is_duplicate_flag",    StringType(),    True),
    StructField("data_quality_flag",    StringType(),    True),
])

bronze_table   = "hospital_analytics.01_bronze.patient_info"
ingestion_ts   = datetime.now()
ingestion_date = ingestion_ts.date()
today_date     = date.today()

try:
    existing_bronze_df = spark.read.format("delta").table(bronze_table)
    existing_patient_info = {
        row["patient_id"]: {
            "first_name": row["first_name"], "last_name": row["last_name"],
            "gender": row["gender"], "age": row["age"],
            "last_visit_date": row["last_visit_date"],
            "primary_diagnosis": row["primary_diagnosis"],
        }
        for row in existing_bronze_df.select(
            "patient_id", "first_name", "last_name", "gender",
            "age", "last_visit_date", "primary_diagnosis"
        ).collect()
    }
    existing_patient_ids = list(existing_patient_info.keys())
    print(f"✅ Found {len(existing_patient_ids):,} existing patients for update simulation.")
except Exception as e:
    print(f"Bronze table not found. First incremental load. Error: {e}")
    existing_patient_info = {}
    existing_patient_ids  = []

# ============================================================
# GENERATE INCREMENTAL RECORDS (20% updates, 80% inserts)
# ============================================================
num_records      = 5_000
num_updates      = int(0.2 * num_records)
num_inserts      = num_records - num_updates
incremental_data = []

# ── Updates to existing patients ──
if existing_patient_ids:
    update_ids = random.sample(existing_patient_ids, min(num_updates, len(existing_patient_ids)))
    for pid in update_ids:
        info = existing_patient_info.get(pid)
        if not info:
            continue
        existing_age        = info["age"]
        existing_last_visit = info["last_visit_date"]
        existing_first_name = info["first_name"]
        existing_last_name  = info["last_name"]
        existing_gender     = info["gender"]

        new_age = random.randint(existing_age, min(existing_age + 2, 120))

        if existing_last_visit is not None:
            start_date_obj = existing_last_visit + timedelta(days=1)
            if start_date_obj > today_date:
                start_date_obj = today_date
        else:
            start_date_obj = today_date - timedelta(days=730)

        new_last_visit = fake.date_between(start_date=start_date_obj, end_date=today_date)
        primary_dx     = get_diagnosis(new_age)
        payment        = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0]
        raw_str        = f"{pid}{existing_first_name}{existing_last_name}{str(new_last_visit)}"

        incremental_data.append({
            "patient_id": pid, "id_number": None,
            "first_name": existing_first_name, "last_name": existing_last_name,
            "age": new_age, "gender": existing_gender,
            "demographic": random.choices(DEMOGRAPHICS, weights=DEMO_WEIGHTS)[0],
            "marital_status": get_marital_status(new_age),
            "preferred_language": random.choices(SA_LANGUAGES, weights=LANG_WEIGHTS)[0],
            "province": random.choice(PROVINCES), "city": random.choice(PRETORIA_AREAS),
            "postal_code": str(random.randint(1, 9999)).zfill(4),
            "primary_diagnosis": primary_dx,
            "secondary_diagnosis": get_secondary_diagnosis(new_age, primary_dx),
            "ward": None, "admission_type": None, "last_visit_date": new_last_visit,
            "discharge_date": None, "length_of_stay_days": None,
            "facility_name": random.choice(FACILITIES), "payment_type": payment,
            "medical_aid_scheme": random.choice(MEDICAL_AIDS) if payment == "Medical Aid" else None,
            "attending_doctor_id": f"DR-{random.randint(1000, 9999)}",
            "source_system": random.choice(SOURCE_SYSTEMS),
            "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
            "record_hash": hashlib.md5(raw_str.encode()).hexdigest(),
            "is_duplicate_flag": "N", "data_quality_flag": "PASS",
        })
else:
    print("No existing records found for update simulation.")

# ── New inserts ──
for _ in range(num_inserts):
    gender     = random.choices(["Male", "Female"], weights=[0.48, 0.52])[0]
    age        = random.randint(0, 95)
    first_name = random.choice(MALE_NAMES if gender == "Male" else FEMALE_NAMES)
    last_name  = random.choice(SA_SURNAMES)
    patient_id = str(uuid.uuid4())
    payment    = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0]
    last_visit = fake.date_between(start_date="-4y", end_date="today")
    primary_dx = get_diagnosis(age)
    raw_str    = f"{patient_id}{first_name}{last_name}{str(last_visit)}"

    incremental_data.append({
        "patient_id": patient_id, "id_number": None,
        "first_name": first_name, "last_name": last_name, "age": age, "gender": gender,
        "demographic": random.choices(DEMOGRAPHICS, weights=DEMO_WEIGHTS)[0],
        "marital_status": get_marital_status(age),
        "preferred_language": random.choices(SA_LANGUAGES, weights=LANG_WEIGHTS)[0],
        "province": random.choice(PROVINCES), "city": random.choice(PRETORIA_AREAS),
        "postal_code": str(random.randint(1, 9999)).zfill(4),
        "primary_diagnosis": primary_dx,
        "secondary_diagnosis": get_secondary_diagnosis(age, primary_dx),
        "ward": None, "admission_type": None, "last_visit_date": last_visit,
        "discharge_date": None, "length_of_stay_days": None,
        "facility_name": random.choice(FACILITIES), "payment_type": payment,
        "medical_aid_scheme": random.choice(MEDICAL_AIDS) if payment == "Medical Aid" else None,
        "attending_doctor_id": f"DR-{random.randint(1000, 9999)}",
        "source_system": random.choice(SOURCE_SYSTEMS),
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": hashlib.md5(raw_str.encode()).hexdigest(),
        "is_duplicate_flag": "N", "data_quality_flag": "PASS",
    })

# ============================================================
# WRITE (APPEND) TO BRONZE
# ============================================================
inc_df = spark.createDataFrame(incremental_data, schema=bronze_schema)

(
    inc_df.write.format("delta").mode("append").saveAsTable(bronze_table)
)

print(f"✅ Incremental Bronze load complete: {len(incremental_data):,} records "
      f"({num_updates} updates + {num_inserts} inserts) | {ingestion_date}")
