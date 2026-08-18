# ============================================================
# CELL 1 : SETUP & READ BRONZE
# ============================================================
from pyspark.sql.functions import (
    col, when, trim, datediff, current_date, lit
)

bronze_res_df = spark.read.format("delta").table("hospital_analytics.01_bronze.resource_allocation")

# ============================================================
# CELL 2 : RESOURCE CATEGORY MAPPING
# (derived from the six groupings used in Bronze generation)
# ============================================================
RESOURCE_CATEGORY_MAP = {
    # Beds & Accommodation
    "General Ward Bed": "Beds & Accommodation", "ICU Bed": "Beds & Accommodation",
    "High Care Bed": "Beds & Accommodation", "Maternity Bed": "Beds & Accommodation",
    "Paediatric Bed": "Beds & Accommodation", "Isolation Bed": "Beds & Accommodation",
    "Day Ward Bed": "Beds & Accommodation",
    
    # Medical Equipment
    "Ventilator": "Medical Equipment", "ECG Machine": "Medical Equipment",
    "Ultrasound Machine": "Medical Equipment", "X-Ray Machine": "Medical Equipment",
    "MRI Scanner": "Medical Equipment", "CT Scanner": "Medical Equipment",
    "Defibrillator": "Medical Equipment", "Infusion Pump": "Medical Equipment",
    "Dialysis Machine": "Medical Equipment", "Pulse Oximeter": "Medical Equipment",
    "Blood Glucose Monitor": "Medical Equipment", "Anaesthesia Machine": "Medical Equipment",
    "Surgical Lamp": "Medical Equipment", "Endoscope": "Medical Equipment",
    
    # Staff
    "Specialist Doctor": "Staff", "Medical Officer": "Staff", "Intern Doctor": "Staff",
    "Registered Nurse": "Staff", "Enrolled Nurse": "Staff", "Scrub Nurse": "Staff",
    "ICU Nurse": "Staff", "Midwife": "Staff", "Paramedic": "Staff",
    "Radiographer": "Staff", "Pharmacist": "Staff", "Physiotherapist": "Staff",
    "Occupational Therapist": "Staff", "Social Worker": "Staff", "Dietician": "Staff",
    
    # Theatre & Surgical
    "Operating Theatre": "Theatre & Surgical", "Surgical Instrument Set": "Theatre & Surgical",
    "Laparoscopic Equipment": "Theatre & Surgical", "Orthopaedic Drill Set": "Theatre & Surgical",
    
    # Pharmacy & Consumables
    "Blood Unit (O+)": "Pharmacy & Consumables", "Blood Unit (A+)": "Pharmacy & Consumables",
    "Blood Unit (B+)": "Pharmacy & Consumables", "Blood Unit (AB+)": "Pharmacy & Consumables",
    "IV Fluid Stock": "Pharmacy & Consumables", "PPE Stock": "Pharmacy & Consumables",
    "Sterile Dressing Pack": "Pharmacy & Consumables",
    
    # Support & Logistics
    "Ambulance": "Support & Logistics", "Patient Transport Wheelchair": "Support & Logistics",
    "Patient Transport Stretcher": "Support & Logistics", "Mortuary Bay": "Support & Logistics",
}

mapping_expr = None
for resource_type, category in RESOURCE_CATEGORY_MAP.items():
    if mapping_expr is None:
        mapping_expr = when(col("resource_type") == resource_type, category)
    else:
        mapping_expr = mapping_expr.when(col("resource_type") == resource_type, category)
mapping_expr = mapping_expr.otherwise("Uncategorised")

# ============================================================
# CELL 3 : SILVER TRANSFORMATIONS
# ============================================================
silver_res_df = (
    bronze_res_df

    # 1. Standardise text fields
    .withColumn("resource_type",   trim(col("resource_type")))
    .withColumn("ward_key",        trim(col("ward_key")))
    .withColumn("resource_status", trim(col("resource_status")))

    # 2. Resource category (derived)
    .withColumn("resource_category", mapping_expr)

    # 3. Assignment duration (days)
    .withColumn(
        "assignment_duration_days",
        when(
            col("assignment_end_date").isNotNull(),
            datediff(col("assignment_end_date"), col("assignment_start_date"))
        ).otherwise(
            datediff(current_date(), col("assignment_start_date"))
        )
    )

    # 4. Duration category (useful for Gold aggregations)
    .withColumn(
        "duration_category",
        when(col("assignment_duration_days") <= 1,  "Same Day")
        .when(col("assignment_duration_days") <= 7,  "Short-term (2-7 days)")
        .when(col("assignment_duration_days") <= 30, "Medium-term (8-30 days)")
        .otherwise("Long-term (30+ days)")
    )

    # 5. Data quality flag
    .withColumn(
        "data_quality_flag",
        when(col("resource_id").isNull(), "FAIL - Missing Resource ID")
        .when(col("assignment_start_date").isNull(), "FAIL - Missing Assignment Start Date")
        .when(
            col("assignment_end_date").isNotNull() &
            (col("assignment_end_date") < col("assignment_start_date")),
            "FAIL - Assignment End Before Start"
        )
        .when(col("patient_id").isNull(), "WARN - Missing Patient ID")
        .when(col("assignment_duration_days") > 365, "WARN - Duration Exceeds 1 Year")
        .otherwise("PASS")
    )

    # 6. Drop Bronze-only metadata
    .drop("record_hash", "ingestion_timestamp")
)

silver_res_df = silver_res_df.dropDuplicates(["resource_id"])

# ============================================================
# CELL 4 : QUARANTINE SPLIT & WRITE
# ============================================================
quarantine_df = silver_res_df.filter(col("data_quality_flag").startswith("FAIL"))
clean_df      = silver_res_df.filter(~col("data_quality_flag").startswith("FAIL"))

print(f"✅ Clean records      : {clean_df.count():,}")
print(f"⚠️  Quarantine records : {quarantine_df.count():,}")

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.quarantine.resource_allocation")
)

(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.02_silver.resource_allocation")
)

print(f"✅ hospital_analytics.02_silver.resource_allocation written")

# ============================================================
# CELL 5 : VALIDATE dim_facility against resource_allocation Silver
# ============================================================
from pyspark.sql.functions import col, md5

dim_facility_check = spark.read.table("hospital_analytics.03_gold.dim_facility")

res_df = spark.read.table("hospital_analytics.02_silver.resource_allocation")
res_unmatched = (
    res_df.withColumn("facility_sk", md5(col("facility_name")))
    .join(dim_facility_check, "facility_sk", "left_anti")
)
print(f"resource_allocation unmatched facilities: {res_unmatched.count()}")
