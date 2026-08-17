# ============================================================
# fact_admissions
# ============================================================
from pyspark.sql.functions import col, md5, date_format, when

adm_df = spark.read.table("hospital_analytics.02_silver.admission_discharge")

fact_admissions = (
    adm_df
    .withColumn("patient_sk",           # ============================================================
# VALIDATE — confirm every foreign key resolves against its dimension
# ============================================================
dim_ward       = spark.read.table("hospital_analytics.03_gold.dim_ward")
dim_diagnosis  = spark.read.table("hospital_analytics.03_gold.dim_diagnosis")
dim_facility   = spark.read.table("hospital_analytics.03_gold.dim_facility")
dim_doctor     = spark.read.table("hospital_analytics.03_gold.dim_doctor")
dim_date       = spark.read.table("hospital_analytics.03_gold.dim_date")

fact = spark.read.table("hospital_analytics.03_gold.fact_admissions")

checks = {
    "admitting_ward_sk":   dim_ward.select("ward_sk"),
    "discharge_ward_sk":   dim_ward.select("ward_sk"),
    "diagnosis_sk":        dim_diagnosis.select("diagnosis_sk"),
    "facility_sk":         dim_facility.select("facility_sk"),
    "admitting_doctor_sk": dim_doctor.select("doctor_sk"),
}

for fk_col, dim_df in checks.items():
    dim_key = dim_df.columns[0]
    unmatched = (
        fact.filter(col(fk_col).isNotNull())
        .join(dim_df.withColumnRenamed(dim_key, fk_col), fk_col, "left_anti")
    )
    print(f"{fk_col} unmatched: {unmatched.count()}")

# discharge_doctor_sk can be legitimately null (still-admitted patients)
unmatched_discharge_dr = (
    fact.filter(col("discharge_doctor_sk").isNotNull())
    .join(dim_doctor.withColumnRenamed("doctor_sk", "discharge_doctor_sk"),
          "discharge_doctor_sk", "left_anti")
)
print(f"discharge_doctor_sk unmatched: {unmatched_discharge_dr.count()}")

# date keys against dim_date
unmatched_adm_date = (
    fact.filter(col("admission_date_sk").isNotNull())
    .join(dim_date.select(col("date_sk").alias("admission_date_sk")), "admission_date_sk", "left_anti")
)
print(f"admission_date_sk unmatched: {unmatched_adm_date.count()}")md5(col("patient_id")))
    .withColumn("admitting_ward_sk",    md5(col("admitting_ward")))
    .withColumn("discharge_ward_sk",    md5(col("discharge_ward")))
    .withColumn("diagnosis_sk",         md5(col("diagnosis")))
    .withColumn("facility_sk",          md5(col("facility_name")))
    .withColumn("admitting_doctor_sk",  md5(col("admitting_doctor_id")))
    .withColumn("discharge_doctor_sk",  md5(col("discharge_doctor_id")))
    .withColumn("admission_date_sk",    date_format(col("admission_date"), "yyyyMMdd").cast("int"))
    .withColumn(
        "discharge_date_sk",
        when(col("discharge_date").isNotNull(),
             date_format(col("discharge_date"), "yyyyMMdd").cast("int"))
        .otherwise(None)
    )
    .select(
        "admission_id", "patient_sk", "admitting_ward_sk", "discharge_ward_sk",
        "diagnosis_sk", "facility_sk", "admitting_doctor_sk", "discharge_doctor_sk",
        "admission_date_sk", "discharge_date_sk",
        "admission_type", "referring_source", "discharge_disposition",
        "admission_status", "length_of_stay_days", "los_category",
        "is_readmission_validated", "icu_flag", "is_weekend_admission"
    )
)

fact_admissions.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.fact_admissions")

print(f"✅ fact_admissions written: {fact_admissions.count():,} rows")
