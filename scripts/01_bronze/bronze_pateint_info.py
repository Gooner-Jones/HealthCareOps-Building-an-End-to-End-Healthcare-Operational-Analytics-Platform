# ============================================================
# CELL 1: SETUP
# ============================================================
%pip install faker

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType
)
from faker import Faker
from datetime import datetime
import random
import uuid
import hashlib

fake = Faker("en_GB")          # en_ZA not supported in Faker; en_GB as neutral fallback
Faker.seed(42)
random.seed(42)

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.01_bronze")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA 01_bronze")

# ============================================================
# CELL 2 : SA-SPECIFIC REFERENCE DATA 
# ============================================================
FACILITIES = [
    "Steve Biko Academic Hospital", "Kalafong Provincial Tertiary Hospital",
    "Tshwane District Hospital", "Unitas Hospital",
    "Little Company of Mary Hospital", "Netcare Montana Hospital",
    "Mediclinic Kloof",
]

PRETORIA_AREAS = [
    "Soshanguve", "Mamelodi", "Atteridgeville", "Ga-Rankuwa",
    "Centurion", "Pretoria Central", "Hatfield", "Menlyn",
    "Silverton", "Mabopane", "Lyttelton", "Arcadia"
]

DEMOGRAPHICS = ["Black African", "White", "Coloured", "Indian/Asian", "Other"]
DEMO_WEIGHTS  = [0.75, 0.15, 0.04, 0.04, 0.02]

MALE_NAMES = [
    "Sipho", "Thabo", "Kagiso", "Lerato", "Tebogo", "Bongani", "Nkosinathi",
    "Siyanda", "Mpho", "Lwazi", "Johan", "Pieter", "Heinrich", "Rohan",
    "Priya", "Aryan", "Devon", "Brandon", "Neo", "Tshepo", "Thomas", "Esau",
    "Thulani", "Werner", "Thabiso", "Sudeshen", "Alfred", "Lehlogonlo",
    "Collen", "Obed", "Jabulani", "Moses", "Peet", "Jan", "Thato"
]
FEMALE_NAMES = [
    "Nomvula", "Zanele", "Thandiwe", "Nompumelelo", "Lerato", "Ofentse", "Boitumelo",
    "Kelebogile", "Noluthando", "Ayanda", "Thembi", "Annelie", "Marietjie",
    "Fatima", "Priya", "Nadia", "Candice", "Cindy", "Simone", "Leani", "Keamogetswe",
    "Palesa", "Kamogelo", "Tshegofatso", "Oreneilwe", "Georginah", "Asanda",
    "Thuli", "Thokozile", "Venessa", "Cathy", "Keletso", "Khomotso", "Keabetswe"
]

SA_SURNAMES = [
    "Dlamini", "Nkosi", "Mokoena", "Mahlangu", "Ndlovu", "Mthembu", "Zulu",
    "Sithole", "Molefe", "Mabunda", "Sibiya", "Van der Merwe", "Botha", "Pretorius",
    "Du Plessis", "Joubert", "Naidoo", "Pillay", "Reddy", "Adams", "Hendricks",
    "September", "Isaacs", "Khumalo", "Shabalala", "Mkhize", "Cele", "Ntuli", "Swanepoel",
    "Khoza", "Solomons", "Swart", "Pieterson", "Lombardt", "Sekhosana"
]

SA_LANGUAGES = [
    "Zulu", "Xhosa", "Afrikaans", "English", "Sepedi",
    "Setswana", "Sesotho", "Tsonga", "Venda", "Ndebele", "Swati"
]
LANG_WEIGHTS = [0.255, 0.198, 0.127, 0.086, 0.092,
                0.081, 0.071, 0.033, 0.023, 0.019, 0.015]

# ============================================================
# CELL 3 : DIAGNOSES, WARDS, ADMISSION TYPES 
# ============================================================
DIAGNOSES = [
    "Hypertension", "Type 2 Diabetes Mellitus", "HIV/AIDS", "Tuberculosis (TB)",
    "Pneumonia", "Acute Gastroenteritis", "Asthma", "Coronary Artery Disease",
    "Stroke / CVA", "Appendicitis", "Malaria", "Chronic Kidney Disease",
    "Anaemia", "Mental Health - Depression", "Mental Health - Schizophrenia",
    "Trauma - MVA", "Trauma - Assault", "Obstetric Complication",
    "Neonatal Jaundice", "Sepsis",
]

WARDS = [
    "Casualty", "ICU", "General Medical", "Surgical", "Paediatrics",
    "Maternity", "Oncology", "Orthopaedics", "Psychiatry", "Cardiology",
    "Isolation / Infectious Disease", "Outpatient"
]

ADMISSION_TYPES = ["Emergency", "Elective", "Maternity", "Referral", "Walk-in"]
ADMISSION_WEIGHTS = [0.40, 0.25, 0.15, 0.12, 0.08]

# ============================================================
# CELL 4 : MEDICAL AID & PAYMENT
# ============================================================
MEDICAL_AID_SCHEMES = [
    "Discovery Health", "GEMS", "Bonitas", "Momentum Health",
    "Medihelp", "Fedhealth", "Bestmed", "Hosmed"
]

SOURCE_SYSTEMS = ["MedTech EMR", "GoodX", "Healthware", "Nexus EMR", "Paper-Digitised"]

PAYMENT_TYPES = ["Medical Aid", "Private (Self-pay)", "Government (Public)", "RAF", "Uninsured"]
PAYMENT_WEIGHTS = [0.30, 0.10, 0.45, 0.05, 0.10]

# ============================================================
# CELL 5 : HELPER FUNCTIONS 
# ============================================================
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

def generate_sa_id(birth_year: int, birth_month: int, birth_day: int, gender: str) -> str:
    yy = str(birth_year)[-2:]
    mm = str(birth_month).zfill(2)
    dd = str(birth_day).zfill(2)
    gender_digit = random.randint(5, 9) if gender == "Male" else random.randint(0, 4)
    seq = random.randint(0, 999)
    return f"{yy}{mm}{dd}{gender_digit}{seq:03d}08{random.randint(0,9)}"

# ============================================================
# CELL 6 : SCHEMA
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

# ============================================================
# CELL 7 : GENERATE RECORDS
# Note: 200,000 patients. If this runs slowly on Free Edition's
# cluster, drop num_records to ~25,000-50,000 for iteration, then
# scale back up once the pipeline is validated end to end.
# ============================================================
num_records  = 200_000
ingestion_ts = datetime.now()
ingestion_date = ingestion_ts.date()

data_bronze = []

for _ in range(num_records):
    gender      = random.choices(["Male", "Female"], weights=[0.48, 0.52])[0]
    demographic = random.choices(DEMOGRAPHICS, weights=DEMO_WEIGHTS)[0]
    age         = random.randint(0, 95)

    first_name = random.choice(MALE_NAMES if gender == "Male" else FEMALE_NAMES)
    last_name  = random.choice(SA_SURNAMES)

    birth_year  = ingestion_date.year - age
    birth_month = random.randint(1, 12)
    birth_day   = random.randint(1, 28)
    sa_id       = generate_sa_id(birth_year, birth_month, birth_day, gender)

    language = random.choices(SA_LANGUAGES, weights=LANG_WEIGHTS)[0]

    primary_dx   = random.choice(DIAGNOSES)
    secondary_dx = random.choice([d for d in DIAGNOSES if d != primary_dx] + [None, None, None])
    ward         = random.choice(WARDS)
    admission    = random.choices(ADMISSION_TYPES, weights=ADMISSION_WEIGHTS)[0]
    last_visit   = fake.date_between(start_date="-4y", end_date="today")

    if random.random() < 0.70:
        los            = random.randint(1, 30)
        discharge_date = last_visit
    else:
        los            = None
        discharge_date = None

    facility      = random.choice(FACILITIES)
    payment       = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0]
    medical_aid   = random.choice(MEDICAL_AID_SCHEMES) if payment == "Medical Aid" else None
    doctor_id     = f"DR-{random.randint(1000, 9999)}"
    source_system = random.choice(SOURCE_SYSTEMS)
    city          = random.choice(PRETORIA_AREAS)
    postal_code   = str(random.randint(1, 9999)).zfill(4)

    record_hash = hashlib.md5(
        f"{sa_id}{first_name}{last_name}{str(last_visit)}".encode()
    ).hexdigest()

    data_bronze.append({
        "patient_id": str(uuid.uuid4()), "id_number": sa_id,
        "first_name": first_name, "last_name": last_name, "age": age,
        "gender": gender, "demographic": demographic,
        "marital_status": get_marital_status(age), "preferred_language": language,
        "city": city, "postal_code": postal_code,
        "primary_diagnosis": primary_dx, "secondary_diagnosis": secondary_dx,
        "ward": ward, "admission_type": admission,
        "last_visit_date": last_visit, "discharge_date": discharge_date,
        "length_of_stay_days": los, "facility_name": facility,
        "payment_type": payment, "medical_aid_scheme": medical_aid,
        "attending_doctor_id": doctor_id, "source_system": source_system,
        "ingestion_date": ingestion_date, "ingestion_timestamp": ingestion_ts,
        "record_hash": record_hash, "is_duplicate_flag": "N", "data_quality_flag": "PASS",
    })

print(f"Generated {num_records:,} records in memory.")

# ============================================================
# CELL 8 : CREATE DATAFRAME & WRITE TO UNITY CATALOG
# ============================================================
df_bronze = spark.createDataFrame(data_bronze, schema=bronze_schema)

(
df_bronze.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("ingestion_date")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.01_bronze.patient_info")
)

print(f"✅ hospital_analytics.01_bronze.patient_info loaded: {num_records:,} records | {ingestion_date}")

print("Setup complete.")
