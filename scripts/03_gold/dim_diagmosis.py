# ============================================================
# CELL 1 : BUILD dim_diagnosis
# Canonical diagnosis list (from Bronze reference data),
# diagnosis_sk derived deterministically from diagnosis name.
# ============================================================
from pyspark.sql.functions import col, md5

DIAGNOSES = [
    "Hypertension", "Type 2 Diabetes Mellitus", "HIV/AIDS", "Tuberculosis (TB)",
    "Pneumonia", "Acute Gastroenteritis", "Asthma", "Coronary Artery Disease",
    "Stroke / CVA", "Appendicitis", "Malaria", "Chronic Kidney Disease",
    "Anaemia", "Mental Health - Depression", "Mental Health - Schizophrenia",
    "Trauma - MVA", "Trauma - Assault", "Obstetric Complication",
    "Neonatal Jaundice", "Sepsis",
]

CHRONIC_CONDITIONS = {
    "Hypertension", "Type 2 Diabetes Mellitus", "HIV/AIDS", "Tuberculosis (TB)",
    "Coronary Artery Disease", "Chronic Kidney Disease",
    "Mental Health - Depression", "Mental Health - Schizophrenia",
}

NOTIFIABLE_DISEASES = {"HIV/AIDS", "Tuberculosis (TB)", "Malaria"}   # NICD-notifiable

HIGH_RISK_COMORBIDITIES = {"HIV/AIDS", "Tuberculosis (TB)", "Type 2 Diabetes Mellitus"}

DIAGNOSIS_GROUP = {
    "Hypertension": "Cardiovascular", "Coronary Artery Disease": "Cardiovascular",
    "Stroke / CVA": "Cardiovascular",
    "Type 2 Diabetes Mellitus": "Endocrine/Metabolic",
    "HIV/AIDS": "Infectious Disease", "Tuberculosis (TB)": "Infectious Disease",
    "Pneumonia": "Infectious Disease", "Malaria": "Infectious Disease",
    "Sepsis": "Infectious Disease",
    "Acute Gastroenteritis": "Gastrointestinal", "Appendicitis": "Gastrointestinal",
    "Asthma": "Respiratory",
    "Chronic Kidney Disease": "Renal", "Anaemia": "Haematological",
    "Mental Health - Depression": "Mental Health",
    "Mental Health - Schizophrenia": "Mental Health",
    "Trauma - MVA": "Trauma", "Trauma - Assault": "Trauma",
    "Obstetric Complication": "Obstetric/Neonatal",
    "Neonatal Jaundice": "Obstetric/Neonatal",
}

diagnosis_rows = [
    (
        name,
        DIAGNOSIS_GROUP.get(name, "Other"),
        "Y" if name in CHRONIC_CONDITIONS else "N",
        "Y" if name in NOTIFIABLE_DISEASES else "N",
        "Y" if name in HIGH_RISK_COMORBIDITIES else "N",
    )
    for name in DIAGNOSES
]

dim_diagnosis = spark.createDataFrame(
    diagnosis_rows,
    schema=["diagnosis_name", "diagnosis_group", "chronic_flag",
            "notifiable_disease_flag", "high_risk_comorbidity_flag"]
).withColumn("diagnosis_sk", md5(col("diagnosis_name")))

dim_diagnosis = dim_diagnosis.select(
    "diagnosis_sk", "diagnosis_name", "diagnosis_group",
    "chronic_flag", "notifiable_disease_flag", "high_risk_comorbidity_flag"
)

dim_diagnosis.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.dim_diagnosis")

print(f"✅ dim_diagnosis written: {dim_diagnosis.count()} diagnoses")
dim_diagnosis.show(truncate=False)

# ============================================================
# CELL 2 : VALIDATE against every Silver table with a diagnosis reference
# ============================================================
dim_diagnosis_check = spark.read.table("hospital_analytics.03_gold.dim_diagnosis")

# patient_info: primary_diagnosis + secondary_diagnosis
pat_df = spark.read.table("hospital_analytics.02_silver.patient_info")

pat_primary_unmatched = (
    pat_df.withColumn("diagnosis_sk", md5(col("primary_diagnosis")))
    .join(dim_diagnosis_check, "diagnosis_sk", "left_anti")
)
print(f"patient_info primary_diagnosis unmatched: {pat_primary_unmatched.count()}")

pat_secondary_unmatched = (
    pat_df.filter(col("secondary_diagnosis").isNotNull())
    .withColumn("diagnosis_sk", md5(col("secondary_diagnosis")))
    .join(dim_diagnosis_check, "diagnosis_sk", "left_anti")
)
print(f"patient_info secondary_diagnosis unmatched: {pat_secondary_unmatched.count()}")

# admission_discharge: diagnosis
adm_df = spark.read.table("hospital_analytics.02_silver.admission_discharge")
adm_unmatched = (
    adm_df.withColumn("diagnosis_sk", md5(col("diagnosis")))
    .join(dim_diagnosis_check, "diagnosis_sk", "left_anti")
)
print(f"admission_discharge diagnosis unmatched: {adm_unmatched.count()}")
