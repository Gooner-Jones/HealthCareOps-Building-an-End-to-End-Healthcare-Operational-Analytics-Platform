# ============================================================
# CELL 1 : incremental_resource_allocation_silver
# ============================================================
from pyspark.sql.functions import col, when, trim, datediff, current_date, lit

bronze_table = "hospital_analytics.01_bronze.resource_allocation"
silver_table = "hospital_analytics.02_silver.resource_allocation"

load_date = spark.sql("SELECT current_date() as current_date").collect()[0]["current_date"]

inc_raw_df = (
    spark.read.format("delta").table(bronze_table)
    .filter(col("ingestion_date") == lit(load_date))
)
print(f"📥 Incremental Bronze records for {load_date}: {inc_raw_df.count():,}")

# Same resource_category mapping as the original full-load Silver script
RESOURCE_CATEGORY_MAP = {
    "General Ward Bed": "Beds & Accommodation", "ICU Bed": "Beds & Accommodation",
    "High Care Bed": "Beds & Accommodation", "Maternity Bed": "Beds & Accommodation",
    "Paediatric Bed": "Beds & Accommodation", "Isolation Bed": "Beds & Accommodation",
    "Day Ward Bed": "Beds & Accommodation",
    "Ventilator": "Medical Equipment", "ECG Machine": "Medical Equipment",
    "Ultrasound Machine": "Medical Equipment", "X-Ray Machine": "Medical Equipment",
    "MRI Scanner": "Medical Equipment", "CT Scanner": "Medical Equipment",
    "Defibrillator": "Medical Equipment", "Infusion Pump": "Medical Equipment",
    "Dialysis Machine": "Medical Equipment", "Pulse Oximeter": "Medical Equipment",
    "Blood Glucose Monitor": "Medical Equipment", "Anaesthesia Machine": "Medical Equipment",
    "Surgical Lamp": "Medical Equipment", "Endoscope": "Medical Equipment",
    "Specialist Doctor": "Staff", "Medical Officer": "Staff", "Intern Doctor": "Staff",
    "Registered Nurse": "Staff", "Enrolled Nurse": "Staff", "Scrub Nurse": "Staff",
    "ICU Nurse": "Staff", "Midwife": "Staff", "Paramedic": "Staff",
    "Radiographer": "Staff", "Pharmacist": "Staff", "Physiotherapist": "Staff",
    "Occupational Therapist": "Staff", "Social Worker": "Staff", "Dietician": "Staff",
    "Operating Theatre": "Theatre & Surgical", "Surgical Instrument Set": "Theatre & Surgical",
    "Laparoscopic Equipment": "Theatre & Surgical", "Orthopaedic Drill Set": "Theatre & Surgical",
    "Blood Unit (O+)": "Pharmacy & Consumables", "Blood Unit (A+)": "Pharmacy & Consumables",
    "Blood Unit (B+)": "Pharmacy & Consumables", "Blood Unit (AB+)": "Pharmacy & Consumables",
    "IV Fluid Stock": "Pharmacy & Consumables", "PPE Stock": "Pharmacy & Consumables",
    "Sterile Dressing Pack": "Pharmacy & Consumables",
    "Ambulance": "Support & Logistics", "Patient Transport Wheelchair": "Support & Logistics",
    "Patient Transport Stretcher": "Support & Logistics", "Mortuary Bay": "Support & Logistics",
}
mapping_expr = None
for rt, cat in RESOURCE_CATEGORY_MAP.items():
    mapping_expr = (when(col("resource_type") == rt, cat) if mapping_expr is None
                     else mapping_expr.when(col("resource_type") == rt, cat))
mapping_expr = mapping_expr.otherwise("Uncategorised")

silver_res_df = (
    inc_raw_df
    .withColumn("resource_type",   trim(col("resource_type")))
    .withColumn("ward_key",        trim(col("ward_key")))
    .withColumn("resource_status", trim(col("resource_status")))
    .withColumn("resource_category", mapping_expr)
    .withColumn(
        "assignment_duration_days",
        when(
            col("assignment_end_date").isNotNull(),
            datediff(col("assignment_end_date"), col("assignment_start_date"))
        ).otherwise(
            datediff(current_date(), col("assignment_start_date"))
        )
    )
    .withColumn(
        "duration_category",
        when(col("assignment_duration_days") <= 1,  "Same Day")
        .when(col("assignment_duration_days") <= 7,  "Short-term (2-7 days)")
        .when(col("assignment_duration_days") <= 30, "Medium-term (8-30 days)")
        .otherwise("Long-term (30+ days)")
    )
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
    .drop("record_hash", "ingestion_timestamp")
)
silver_res_df = silver_res_df.dropDuplicates(["resource_id"])

quarantine_df = silver_res_df.filter(col("data_quality_flag").startswith("FAIL"))
clean_df      = silver_res_df.filter(~col("data_quality_flag").startswith("FAIL"))

print(f"✅ Clean records      : {clean_df.count():,}")
print(f"⚠️  Quarantine records : {quarantine_df.count():,}")

quarantine_df.write.format("delta").mode("append").saveAsTable("hospital_analytics.quarantine.resource_allocation")

# ============================================================
# CELL 2 : UPSERT INTO SILVER
# ============================================================
from delta.tables import DeltaTable

silver_delta = DeltaTable.forName(spark, silver_table)

(
    silver_delta.alias("t")
    .merge(clean_df.alias("s"), "t.resource_id = s.resource_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✅ Upserted {clean_df.count():,} records into {silver_table} | {load_date}")

