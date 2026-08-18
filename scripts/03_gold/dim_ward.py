# ============================================================
# CELL 1 : dim_ward with network-wide capacity values
# (self-contained — doesn't depend on any other notebook's session)
# ============================================================
from pyspark.sql.functions import col, md5

NUM_FACILITIES = 7

WARD_CAPACITIES_PER_FACILITY = {
    "Casualty": 30, "ICU": 12, "General Medical": 60, "Surgical": 40,
    "Paediatrics": 35, "Maternity": 40, "Oncology": 25, "Orthopaedics": 30,
    "Psychiatry": 30, "Cardiology": 20, "Isolation / Infectious Disease": 20,
    "Outpatient": 50,
}

WARD_CAPACITIES = {
    ward: cap * NUM_FACILITIES
    for ward, cap in WARD_CAPACITIES_PER_FACILITY.items()
}

WARD_DEFAULT_BED_TYPE = {
    "ICU": "ICU Bed", "Casualty": "High Care Bed", "Maternity": "Maternity Bed",
    "Paediatrics": "Paediatric Bed", "Isolation / Infectious Disease": "Isolation Bed",
    "General Medical": "General Ward Bed", "Surgical": "General Ward Bed",
    "Oncology": "General Ward Bed", "Orthopaedics": "General Ward Bed",
    "Psychiatry": "General Ward Bed", "Cardiology": "High Care Bed",
    "Outpatient": "Day Ward Bed",
}

ward_rows = [
    (ward_name, capacity, WARD_DEFAULT_BED_TYPE.get(ward_name, "General Ward Bed"))
    for ward_name, capacity in WARD_CAPACITIES.items()
]

dim_ward = spark.createDataFrame(
    ward_rows, schema=["ward_name", "ward_capacity", "default_bed_type"]
).withColumn("ward_sk", md5(col("ward_name")))

dim_ward = dim_ward.select("ward_sk", "ward_name", "ward_capacity", "default_bed_type")

dim_ward.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.dim_ward")

print(f"✅ dim_ward updated with network-wide capacities: {dim_ward.count()} wards")
dim_ward.show(truncate=False)
