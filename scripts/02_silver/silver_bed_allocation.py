# ============================================================
# CELL 1 : SETUP & READ BRONZE
# ============================================================
from pyspark.sql.functions import (
    col, when, trim, datediff, current_date
)

bronze_bed_df = spark.read.format("delta").table("hospital_analytics.01_bronze.bed_allocation")

# ============================================================
# CELL 2 : SILVER TRANSFORMATIONS
# ============================================================
silver_bed_df = (
    bronze_bed_df

    # 1. Standardise text fields
    .withColumn("ward_key",    trim(col("ward_key")))
    .withColumn("bed_type",    trim(col("bed_type")))
    .withColumn("bed_number",  trim(col("bed_number")))
    .withColumn("bed_id",      trim(col("bed_id")))

    # 2. Re-derive length of stay (don't trust Bronze's version blindly)
    .withColumn(
        "length_of_stay_days",
        when(
            col("discharge_date").isNotNull(),
            datediff(col("discharge_date"), col("admission_date"))
        ).otherwise(
            datediff(current_date(), col("admission_date"))
        )
    )

    # 3. LOS category (consistent with admission_discharge Silver)
    .withColumn(
        "los_category",
        when(col("length_of_stay_days") <= 1,  "Same Day")
        .when(col("length_of_stay_days") <= 3,  "Short Stay (2-3 days)")
        .when(col("length_of_stay_days") <= 7,  "Medium Stay (4-7 days)")
        .when(col("length_of_stay_days") <= 14, "Long Stay (8-14 days)")
        .otherwise("Extended Stay (15+ days)")
    )

    # 4. Re-derive bed status from discharge_date (don't trust Bronze's flag)
    .withColumn(
        "bed_status",
        when(col("discharge_date").isNotNull(), "Available")
        .otherwise("Occupied")
    )

    # 5. Data quality flag — re-validated at Silver, supersedes Bronze's flag
    .withColumn(
        "data_quality_flag",
        when(col("admission_id").isNull(), "FAIL - Missing Admission ID")
        .when(col("patient_id").isNull(), "FAIL - Missing Patient ID")
        .when(col("admission_date").isNull(), "FAIL - Missing Admission Date")
        .when(
            col("discharge_date").isNotNull() &
            (col("discharge_date") < col("admission_date")),
            "FAIL - Discharge Before Admission"
        )
        .when(col("length_of_stay_days") > 365, "WARN - LOS Exceeds 1 Year")
        .otherwise("PASS")
    )

    # 6. Drop Bronze-only metadata
    .drop("record_hash", "is_duplicate_flag", "ingestion_timestamp")
)

silver_bed_df = silver_bed_df.dropDuplicates(["bed_allocation_id"])

# ============================================================
# CELL 3 : QUARANTINE SPLIT & WRITE
# ============================================================
quarantine_df = silver_bed_df.filter(col("data_quality_flag").startswith("FAIL"))
clean_df      = silver_bed_df.filter(~col("data_quality_flag").startswith("FAIL"))

print(f"✅ Clean records      : {clean_df.count():,}")
print(f"⚠️  Quarantine records : {quarantine_df.count():,}")

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.quarantine.bed_allocation")
)

(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.silver.bed_allocation")
)

print(f"✅ hospital_analytics.02_silver.bed_allocation written")
