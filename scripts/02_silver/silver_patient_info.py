# ============================================================
# CELL 1 : SETUP
# ============================================================
from pyspark.sql.functions import (
    col, when, lit, trim, upper, lower,
    current_date, current_timestamp,
    year, month, regexp_replace, length,
    to_date, datediff, floor
)

spark.sql("CREATE SCHEMA IF NOT EXISTS hospital_analytics.02_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS hospital_analytics.quarantine")

print("Setup complete.")

# ============================================================
# CELL 2 : READ BRONZE
# ============================================================
bronze_table = "hospital_analytics.01_bronze.patient_info"
silver_table = "hospital_analytics.02_silver.patient_info"

bronze_df = spark.read.format("delta").table(01_bronze_table)

# ============================================================
# CELL 3 : SILVER TRANSFORMATIONS
# ============================================================
silver_df = (
    bronze_df

    # 1. Standardise text casing & trim whitespace
    .withColumn("first_name",         trim(col("first_name")))
    .withColumn("last_name",          trim(col("last_name")))
    .withColumn("gender",             trim(col("gender")))
    .withColumn("demographic",        trim(col("demographic")))
    .withColumn("marital_status",     trim(col("marital_status")))
    .withColumn("city",               trim(col("city")))
    .withColumn("primary_diagnosis",  trim(col("primary_diagnosis")))
    .withColumn("preferred_language", trim(col("preferred_language")))
    .withColumn("facility_name",      trim(col("facility_name")))
    .withColumn("payment_type",       trim(col("payment_type")))

    # 2. Standardise gender
    .withColumn(
        "gender",
        when(upper(col("gender")) == "MALE",   "Male")
        .when(upper(col("gender")) == "FEMALE", "Female")
        .otherwise("Unknown")
    )

    # 3. Age validation & banding
    .withColumn(
        "age_valid_flag",
        when((col("age") >= 0) & (col("age") <= 120), "Y")
        .otherwise("N")
    )
    .withColumn(
        "age_band",
        when(col("age") <= 12,  "Child (0-12)")
        .when(col("age") <= 17,  "Adolescent (13-17)")
        .when(col("age") <= 35,  "Young Adult (18-35)")
        .when(col("age") <= 59,  "Middle Aged (36-59)")
        .when(col("age") <= 74,  "Senior (60-74)")
        .otherwise("Elderly (75+)")
    )

    # 4. SA ID number validation
    .withColumn(
        "id_number_valid_flag",
        when(
            (length(regexp_replace(col("id_number"), "[^0-9]", "")) == 13),
            "Y"
        ).otherwise("N")
    )

    # 5. Payment type grouping
    .withColumn(
        "funding_category",
        when(col("payment_type") == "Government (Public)", "Public")
        .when(
            col("payment_type").isin(["Medical Aid", "Private (Self-pay)"]),
            "Private"
        )
        .when(col("payment_type") == "RAF", "RAF")
        .otherwise("Uninsured / Other")
    )

    # 6. Comorbidity flags
    .withColumn(
        "has_comorbidity",
        when(col("secondary_diagnosis").isNotNull(), "Y")
        .otherwise("N")
    )
    .withColumn(
        "high_risk_comorbidity_flag",
        when(
            col("primary_diagnosis").isin(["HIV/AIDS", "Tuberculosis (TB)"]) |
            col("secondary_diagnosis").isin(["HIV/AIDS", "Tuberculosis (TB)",
                                             "Type 2 Diabetes Mellitus"]),
            "Y"
        ).otherwise("N")
    )

    # 7. SCD Type 2 fields
    .withColumn("effective_start_date", current_date())
    .withColumn("effective_end_date",   lit(None).cast("date"))
    .withColumn("is_current",           lit("Y"))

    # 8. Data quality flag
    .withColumn(
        "data_quality_flag",
        when(col("patient_id").isNull(),
             "FAIL - Missing Patient ID")
        .when(col("first_name").isNull() | col("last_name").isNull(),
             "FAIL - Missing Name")
        .when(col("age_valid_flag") == "N",
             "FAIL - Invalid Age")
        .when(col("id_number_valid_flag") == "N",
             "WARN - Invalid ID Number")
        .when(col("gender") == "Unknown",
             "WARN - Unknown Gender")
        .otherwise("PASS")
    )

    # 9. Drop Bronze-only metadata
    .drop("record_hash", "is_duplicate_flag", "ingestion_timestamp",
          "age_valid_flag", "id_number_valid_flag")
)

silver_df = silver_df.dropDuplicates(["patient_id"])

quarantine_df = silver_df.filter(col("data_quality_flag").startswith("FAIL"))
clean_df      = silver_df.filter(~col("data_quality_flag").startswith("FAIL"))

print(f"✅ Clean records      : {clean_df.count():,}")
print(f"⚠️  Quarantine records : {quarantine_df.count():,}")

# ============================================================
# CELL 4 : READ BRONZE
# ============================================================
bronze_table = "hospital_analytics.01_bronze.patient_info"
silver_table = "hospital_analytics.02_silver.patient_info"

bronze_df = spark.read.format("delta").table(01_bronze_table)

# ============================================================
# CELL 5 : WRITE QUARANTINE & SILVER TABLES
# ============================================================
(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.quarantine.patient_info")
)

(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"✅ {silver_table} written successfully")
