# ============================================================
# SETUP & READ TODAY'S INCREMENTAL BRONZE RECORDS
# ============================================================
from pyspark.sql.functions import col, when, trim, datediff, current_date, lit
from delta.tables import DeltaTable

bronze_table = "hospital_analytics.01_bronze.bed_allocation"
silver_table = "hospital_analytics.02_silver.bed_allocation"

load_date = spark.sql("SELECT current_date() as current_date").collect()[0]["current_date"]

inc_raw_df = (
    spark.read.format("delta").table(bronze_table)
    .filter(col("ingestion_date") == lit(load_date))
)
print(f"📥 Incremental Bronze records for {load_date}: {inc_raw_df.count():,}")


# ============================================================
# SILVER TRANSFORMATIONS (same logic as the original
# full-load Silver script — re-derives LOS and status from dates
# rather than trusting Bronze's version)
# ============================================================
silver_bed_df = (
    inc_raw_df

    .withColumn("ward_key",    trim(col("ward_key")))
    .withColumn("bed_type",    trim(col("bed_type")))
    .withColumn("bed_number",  trim(col("bed_number")))
    .withColumn("bed_id",      trim(col("bed_id")))

    .withColumn(
        "length_of_stay_days",
        when(
            col("discharge_date").isNotNull(),
            datediff(col("discharge_date"), col("admission_date"))
        ).otherwise(
            datediff(current_date(), col("admission_date"))
        )
    )

    .withColumn(
        "los_category",
        when(col("length_of_stay_days") <= 1,  "Same Day")
        .when(col("length_of_stay_days") <= 3,  "Short Stay (2-3 days)")
        .when(col("length_of_stay_days") <= 7,  "Medium Stay (4-7 days)")
        .when(col("length_of_stay_days") <= 14, "Long Stay (8-14 days)")
        .otherwise("Extended Stay (15+ days)")
    )

    .withColumn(
        "bed_status",
        when(col("discharge_date").isNotNull(), "Available")
        .otherwise("Occupied")
    )

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

    .drop("record_hash", "is_duplicate_flag", "ingestion_timestamp")
)

# ============================================================
# QUARANTINE SPLIT
# ============================================================
quarantine_df = silver_bed_df.filter(col("data_quality_flag").startswith("FAIL"))
clean_df      = silver_bed_df.filter(~col("data_quality_flag").startswith("FAIL"))

print(f"✅ Clean records      : {clean_df.count():,}")
print(f"⚠️  Quarantine records : {quarantine_df.count():,}")

(
    quarantine_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("hospital_analytics.quarantine.bed_allocation")
)

# ============================================================
# UPSERT INTO SILVER (plain merge on bed_allocation_id —
# same pattern as admission_discharge Silver, not SCD2)
# ============================================================
silver_delta = DeltaTable.forName(spark, silver_table)

(
    silver_delta.alias("t")
    .merge(clean_df.alias("s"), "t.bed_allocation_id = s.bed_allocation_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

updated_or_inserted = clean_df.count()
print(f"✅ Upserted {updated_or_inserted:,} records into {silver_table} | {load_date}")


silver_bed_df = silver_bed_df.dropDuplicates(["bed_allocation_id"])
