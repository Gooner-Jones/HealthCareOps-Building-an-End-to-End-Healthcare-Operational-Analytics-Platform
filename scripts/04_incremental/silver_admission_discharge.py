# ============================================================
# SETUP & READ TODAY'S INCREMENTAL BRONZE RECORDS
# ============================================================
from pyspark.sql.functions import (
    col, when, datediff, year, month, quarter,
    current_date, to_timestamp, concat_ws,
    dayofweek, lit, trim, upper, regexp_replace
)
from delta.tables import DeltaTable

bronze_table = "hospital_analytics.01_bronze.admission_discharge"
silver_table = "hospital_analytics.02_silver.admission_discharge"

load_date = spark.sql("SELECT current_date() as current_date").collect()[0]["current_date"]

inc_raw_df = (
    spark.read.format("delta").table(bronze_table)
    .filter(col("ingestion_date") == lit(load_date))
)
print(f"📥 Incremental Bronze records for {load_date}: {inc_raw_df.count():,}")

# ============================================================
# CELL 2 — SILVER TRANSFORMATIONS (same logic as the original
# full-load Silver script — every incremental record must go
# through identical transformation, or schemas/logic will drift)
# ============================================================
silver_adm_df = (
    inc_raw_df

    .withColumn(
        "admission_status",
        when(col("discharge_date").isNotNull(), "Discharged")
        .otherwise("Active")
    )

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

    .withColumn("admission_year",    year(col("admission_date")))
    .withColumn("admission_month",   month(col("admission_date")))
    .withColumn("admission_quarter", quarter(col("admission_date")))
    .withColumn("admission_day_of_week", dayofweek(col("admission_date")))
    .withColumn(
        "is_weekend_admission",
        when(dayofweek(col("admission_date")).isin([1, 7]), "Y")
        .otherwise("N")
    )

    .withColumn(
        "admission_datetime",
        to_timestamp(
            concat_ws(" ", col("admission_date").cast("string"), col("admission_time")),
            "yyyy-MM-dd HH:mm"
        )
    )
    .withColumn(
        "discharge_datetime",
        when(
            col("discharge_date").isNotNull() & col("discharge_time").isNotNull(),
            to_timestamp(
                concat_ws(" ", col("discharge_date").cast("string"), col("discharge_time")),
                "yyyy-MM-dd HH:mm"
            )
        ).otherwise(lit(None).cast("timestamp"))
    )

    .withColumn("admitting_ward",   trim(col("admitting_ward")))
    .withColumn("admission_type",   trim(col("admission_type")))
    .withColumn("facility_name",    trim(col("facility_name")))
    .withColumn("diagnosis",        trim(col("diagnosis")))

    .withColumn(
        "is_readmission_validated",
        when(col("is_readmission") == "Y", "Y")
        .otherwise("N")
    )

    .withColumn(
        "data_quality_flag",
        when(col("admission_date").isNull(), "FAIL - Missing Admission Date")
        .when(
            col("discharge_date").isNotNull() &
            (col("discharge_date") < col("admission_date")),
            "FAIL - Discharge Before Admission"
        )
        .when(col("patient_id").isNull(), "FAIL - Missing Patient ID")
        .when(col("length_of_stay_days") > 365, "WARN - LOS Exceeds 1 Year")
        .otherwise("PASS")
    )

    .drop("record_hash", "is_duplicate_flag", "ingestion_timestamp")
)

silver_adm_df = silver_adm_df.dropDuplicates(["admission_id"])

# ============================================================
# QUARANTINE SPLIT
# ============================================================
quarantine_df = silver_adm_df.filter(col("data_quality_flag").startswith("FAIL"))
clean_df      = silver_adm_df.filter(~col("data_quality_flag").startswith("FAIL"))

print(f"✅ Clean records      : {clean_df.count():,}")
print(f"⚠️  Quarantine records : {quarantine_df.count():,}")

(
    quarantine_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("hospital_analytics.quarantine.admission_discharge")
)

# ============================================================
# UPSERT INTO SILVER (not SCD2 — plain merge on admission_id)
# Existing admission_ids get updated (discharge info arrived);
# new admission_ids get inserted.
# ============================================================
silver_delta = DeltaTable.forName(spark, silver_table)

(
    silver_delta.alias("t")
    .merge(clean_df.alias("s"), "t.admission_id = s.admission_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

updated_or_inserted = clean_df.count()
print(f"✅ Upserted {updated_or_inserted:,} records into {silver_table} | {load_date}")
