# ============================================================
# CELL 1 : BUILD dim_facility
# ============================================================
from pyspark.sql.functions import col, md5

FACILITIES = [
    "Steve Biko Academic Hospital", "Kalafong Provincial Tertiary Hospital",
    "Tshwane District Hospital", "Unitas Hospital",
    "Little Company of Mary Hospital", "Netcare Montana Hospital", "Mediclinic Kloof",
]

FACILITY_TYPE = {
    "Steve Biko Academic Hospital": "Academic/Tertiary",
    "Kalafong Provincial Tertiary Hospital": "Academic/Tertiary",
    "Tshwane District Hospital": "Public District",
    "Unitas Hospital": "Private",
    "Little Company of Mary Hospital": "Private",
    "Netcare Montana Hospital": "Private",
    "Mediclinic Kloof": "Private",
}

FACILITY_SECTOR = {
    "Steve Biko Academic Hospital": "Public",
    "Kalafong Provincial Tertiary Hospital": "Public",
    "Tshwane District Hospital": "Public",
    "Unitas Hospital": "Private",
    "Little Company of Mary Hospital": "Private",
    "Netcare Montana Hospital": "Private",
    "Mediclinic Kloof": "Private",
}

facility_rows = [
    (name, FACILITY_TYPE.get(name, "Other"), FACILITY_SECTOR.get(name, "Other"))
    for name in FACILITIES
]

dim_facility = spark.createDataFrame(
    facility_rows, schema=["facility_name", "facility_type", "sector"]
).withColumn("facility_sk", md5(col("facility_name")))

dim_facility = dim_facility.select("facility_sk", "facility_name", "facility_type", "sector")

dim_facility.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.dim_facility")

print(f"✅ dim_facility written: {dim_facility.count()} facilities")
dim_facility.show(truncate=False)

# ============================================================
# CELL 2 : VALIDATE against every Silver table with a facility reference
# ============================================================
dim_facility_check = spark.read.table("hospital_analytics.03_gold.dim_facility")

pat_df = spark.read.table("hospital_analytics.02_silver.patient_info")
pat_unmatched = (
    pat_df.withColumn("facility_sk", md5(col("facility_name")))
    .join(dim_facility_check, "facility_sk", "left_anti")
)
print(f"patient_info unmatched facilities: {pat_unmatched.count()}")

adm_df = spark.read.table("hospital_analytics.02_silver.admission_discharge")
adm_unmatched = (
    adm_df.withColumn("facility_sk", md5(col("facility_name")))
    .join(dim_facility_check, "facility_sk", "left_anti")
)
print(f"admission_discharge unmatched facilities: {adm_unmatched.count()}")
