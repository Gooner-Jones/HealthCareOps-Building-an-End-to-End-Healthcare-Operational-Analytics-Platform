# ============================================================
# fact_patient_demographics
# ============================================================
from pyspark.sql.functions import col, md5, when

pat_df = spark.read.table("hospital_analytics.02_silver.patient_info")

fact_patient_demographics = (
    pat_df
    .withColumn("patient_sk",             md5(col("patient_id")))
    .withColumn("primary_diagnosis_sk",   md5(col("primary_diagnosis")))
    .withColumn(
        "secondary_diagnosis_sk",
        when(col("secondary_diagnosis").isNotNull(), md5(col("secondary_diagnosis")))
        .otherwise(None)
    )
    .withColumn("facility_sk",            md5(col("facility_name")))
    .withColumn("payment_sk",             md5(col("payment_type")))
    .select(
        "patient_id", "patient_sk", "primary_diagnosis_sk", "secondary_diagnosis_sk",
        "facility_sk", "payment_sk",
        "age", "age_band", "gender", "demographic", "marital_status",
        "preferred_language", "city", "postal_code",
        "medical_aid_scheme", "funding_category",
        "has_comorbidity", "high_risk_comorbidity_flag"
    )
)

fact_patient_demographics.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.fact_patient_demographics")

print(f"✅ fact_patient_demographics written: {fact_patient_demographics.count():,} rows")
