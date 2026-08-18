# ============================================================
# CELL 1 : fact_resource_allocation
# ============================================================
from pyspark.sql.functions import col, md5, date_format, when

res_df = spark.read.table("hospital_analytics.02_silver.resource_allocation")

fact_resource_allocation = (
    res_df
    .withColumn("patient_sk",  md5(col("patient_id")))
    .withColumn("ward_sk",     md5(col("ward_key")))
    .withColumn("facility_sk", md5(col("facility_name")))
    .withColumn(
        "assignment_start_date_sk",
        date_format(col("assignment_start_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "assignment_end_date_sk",
        when(col("assignment_end_date").isNotNull(),
             date_format(col("assignment_end_date"), "yyyyMMdd").cast("int"))
        .otherwise(None)
    )
    .select(
        "resource_id", "patient_sk", "ward_sk", "facility_sk",
        "assignment_start_date_sk", "assignment_end_date_sk",
        "resource_type", "resource_category", "resource_status",
        "assignment_duration_days", "duration_category"
    )
)

fact_resource_allocation.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.fact_resource_allocation")

print(f"✅ fact_resource_allocation written: {fact_resource_allocation.count():,} rows")

# ============================================================
# CELL 2 : VALIDATE
# ============================================================
dim_ward     = spark.read.table("hospital_analytics.03_gold.dim_ward").select(col("ward_sk"))
dim_facility = spark.read.table("hospital_analytics.03_gold.dim_facility").select(col("facility_sk"))
dim_date     = spark.read.table("hospital_analytics.03_gold.dim_date").select(col("date_sk"))

fact = spark.read.table("hospital_analytics.03_gold.fact_resource_allocation")

unmatched_ward = fact.filter(col("ward_sk").isNotNull()).join(dim_ward, "ward_sk", "left_anti")
print(f"ward_sk unmatched: {unmatched_ward.count()}")

unmatched_facility = (
    fact.filter(col("facility_sk").isNotNull())
    .join(dim_facility, "facility_sk", "left_anti")
)
print(f"facility_sk unmatched: {unmatched_facility.count()}")

unmatched_start_date = (
    fact.filter(col("assignment_start_date_sk").isNotNull())
    .join(dim_date.withColumnRenamed("date_sk", "assignment_start_date_sk"),
          "assignment_start_date_sk", "left_anti")
)
print(f"assignment_start_date_sk unmatched: {unmatched_start_date.count()}")
