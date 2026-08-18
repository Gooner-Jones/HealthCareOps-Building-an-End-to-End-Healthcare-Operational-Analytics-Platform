# ============================================================
# CELL 1 : fact_bed_utilisation
# ============================================================
from pyspark.sql.functions import col, md5, date_format, when

bed_df = spark.read.table("hospital_analytics.02_silver.bed_allocation")
adm_df = spark.read.table("hospital_analytics.02_silver.admission_discharge") \
    .select("admission_id", "facility_name")

fact_bed_utilisation = (
    bed_df
    .join(adm_df, "admission_id", "left")     # pull facility_name in via admission_id
    .withColumn("patient_sk",     md5(col("patient_id")))
    .withColumn("ward_sk",        md5(col("ward_key")))
    .withColumn("facility_sk",    md5(col("facility_name")))
    .withColumn(
        "admission_date_sk",
        date_format(col("admission_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "discharge_date_sk",
        when(col("discharge_date").isNotNull(),
             date_format(col("discharge_date"), "yyyyMMdd").cast("int"))
        .otherwise(None)
    )
    .select(
        "bed_allocation_id", "admission_id", "patient_sk", "ward_sk", "facility_sk",
        "admission_date_sk", "discharge_date_sk",
        "bed_id", "bed_type", "bed_number", "bed_status",
        "length_of_stay_days", "los_category"
    )
)

fact_bed_utilisation.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.fact_bed_utilisation")

print(f"✅ fact_bed_utilisation written: {fact_bed_utilisation.count():,} rows")

# ============================================================
# CELL 2 : VALIDATE
# ============================================================
dim_ward     = spark.read.table("hospital_analytics.03_gold.dim_ward").select(col("ward_sk"))
dim_facility = spark.read.table("hospital_analytics.03_gold.dim_facility").select(col("facility_sk"))
dim_date     = spark.read.table("hospital_analytics.03_gold.dim_date").select(col("date_sk"))

fact = spark.read.table("hospital_analytics.03_gold.fact_bed_utilisation")

unmatched_ward = fact.filter(col("ward_sk").isNotNull()).join(dim_ward, "ward_sk", "left_anti")
print(f"ward_sk unmatched: {unmatched_ward.count()}")

unmatched_facility = (
    fact.filter(col("facility_sk").isNotNull())
    .join(dim_facility, "facility_sk", "left_anti")
)
print(f"facility_sk unmatched: {unmatched_facility.count()}")

unmatched_adm_date = (
    fact.filter(col("admission_date_sk").isNotNull())
    .join(dim_date.withColumnRenamed("date_sk", "admission_date_sk"), "admission_date_sk", "left_anti")
)
print(f"admission_date_sk unmatched: {unmatched_adm_date.count()}")
