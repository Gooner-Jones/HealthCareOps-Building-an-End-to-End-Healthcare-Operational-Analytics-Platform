# ============================================================
# CELL 1 : BUILD dim_doctor (fixed roster, not random-per-record)
# ============================================================
import random
from pyspark.sql.functions import col, md5

random.seed(42)

SPECIALTIES = [
    "General Practitioner", "Surgeon", "Physician", "Paediatrician",
    "Obstetrician/Gynaecologist", "Psychiatrist", "Cardiologist",
    "Orthopaedic Surgeon", "Oncologist", "Anaesthetist",
    "Emergency Medicine", "Intensivist", "Radiologist", "Nephrologist"
]

SPECIALTY_WEIGHTS = [4.0, 2.5, 3.0, 2.0, 2.0, 1.5, 1.5,
                     1.5, 1.0, 2.0, 3.0, 1.5, 1.0, 1.0]

NUM_DOCTORS = 200

doctor_rows = []
used_ids = set()

while len(doctor_rows) < NUM_DOCTORS:
    doctor_id = f"DR-{random.randint(1000, 9999)}"
    if doctor_id in used_ids:
        continue
    used_ids.add(doctor_id)

    specialty = random.choices(SPECIALTIES, weights=SPECIALTY_WEIGHTS)[0]
    hpcsa_number = f"MP{random.randint(100000, 999999)}"
    years_experience = random.randint(1, 35)

    doctor_rows.append((doctor_id, specialty, hpcsa_number, years_experience))

dim_doctor = spark.createDataFrame(
    doctor_rows,
    schema=["doctor_id", "specialty", "hpcsa_number", "years_experience"]
).withColumn("doctor_sk", md5(col("doctor_id")))

dim_doctor = dim_doctor.select(
    "doctor_sk", "doctor_id", "specialty", "hpcsa_number", "years_experience"
)

dim_doctor.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.dim_doctor")

print(f"✅ dim_doctor written: {dim_doctor.count()} doctors")
dim_doctor.show(10, truncate=False)
