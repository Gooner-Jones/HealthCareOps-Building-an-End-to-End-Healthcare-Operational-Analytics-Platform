# ============================================================
# BUILD dim_ward
# Canonical ward list + capacities (from Bronze reference data),
# with ward_sk derived deterministically from ward_name.
# ============================================================
WARD_CAPACITIES = {
    "Casualty": 30, "ICU": 12, "General Medical": 60, "Surgical": 40,
    "Paediatrics": 35, "Maternity": 40, "Oncology": 25, "Orthopaedics": 30,
    "Psychiatry": 30, "Cardiology": 20, "Isolation / Infectious Disease": 20,
    "Outpatient": 50,
}

WARD_DEFAULT_BED_TYPE = {
    "ICU":                           "ICU Bed",
    "Casualty":                      "High Care Bed",
    "Maternity":                     "Maternity Bed",
    "Paediatrics":                   "Paediatric Bed",
    "Isolation / Infectious Disease":"Isolation Bed",
    "General Medical":               "General Ward Bed",
    "Surgical":                      "General Ward Bed",
    "Oncology":                      "General Ward Bed",
    "Orthopaedics":                  "General Ward Bed",
    "Psychiatry":                    "General Ward Bed",
    "Cardiology":                    "High Care Bed",
    "Outpatient":                    "Day Ward Bed",
}

ward_rows = [
    (ward_name, capacity, WARD_DEFAULT_BED_TYPE.get(ward_name, "General Ward Bed"))
    for ward_name, capacity in WARD_CAPACITIES.items()
]

dim_ward = spark.createDataFrame(
    ward_rows, schema=["ward_name", "ward_capacity", "default_bed_type"]
).withColumn("ward_sk", md5(col("ward_name")))

dim_ward = dim_ward.select("ward_sk", "ward_name", "ward_capacity", "default_bed_type")

dim_ward.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true") \
        .saveAsTable("hospital_analytics.03_gold.dim_ward")

print(f"✅ dim_ward written: {dim_ward.count()} wards")
dim_ward.show(truncate=False)# ============================================================
# VALIDATE: prove the join actually resolves cleanly
# against every Silver table that carries a ward reference
# ============================================================
dim_ward_check = spark.read.table("hospital_analytics.03_gold.dim_ward")

# admission_discharge: admitting_ward
adm_df = spark.read.table("hospital_analytics.02_silver.admission_discharge")
adm_unmatched = (
    adm_df.withColumn("ward_sk", md5(col("admitting_ward")))
    .join(dim_ward_check, "ward_sk", "left_anti")
)
print(f"admission_discharge unmatched wards: {adm_unmatched.count()}")

# resource_allocation: ward_key
res_df = spark.read.table("hospital_analytics.02_silver.resource_allocation")
res_unmatched = (
    res_df.withColumn("ward_sk", md5(col("ward_key")))
    .join(dim_ward_check, "ward_sk", "left_anti")
)
print(f"resource_allocation unmatched wards: {res_unmatched.count()}")

# bed_allocation: ward_key
bed_df = spark.read.table("hospital_analytics.02_silver.bed_allocation")
bed_unmatched = (
    bed_df.withColumn("ward_sk", md5(col("ward_key")))
    .join(dim_ward_check, "ward_sk", "left_anti")
)
print(f"bed_allocation unmatched wards: {bed_unmatched.count()}")
