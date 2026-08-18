# ============================================================
# CELL 1 : SETUP & READ BRONZE
# ============================================================
from pyspark.sql.functions import (
    col, when, datediff, year, month, quarter,
    current_date, to_timestamp, concat_ws,
    dayofweek, lit, trim, upper, regexp_replace
)

bronze_adm_df = spark.read.format("delta").table("hospital_analytics.01_bronze.admission_discharge")

# ============================================================
# CELL 2 : SILVER TRANSFORMATIONS 
# ============================================================
silver_adm_df = (
    bronze_adm_df

    # 1. Fix admission_status logic
    .withColumn(
        "admission_status",
        when(col("discharge_date").isNotNull(), "Discharged")
        .otherwise("Active")
    )

    # 2. Length of stay (days)
    .withColumn(
        "length_of_stay_days",
        when(
            col("discharge_date").isNotNull(),
            datediff(col("discharge_date"), col("admission_date"))
        ).otherwise(
            datediff(current_date(), col("admission_date"))
        )
    )

    # 3. LOS category
    .withColumn(
        "los_category",
        when(col("length_of_stay_days") <= 1,  "Same Day")
        .when(col("length_of_stay_days") <= 3,  "Short Stay (2-3 days)")
        .when(col("length_of_stay_days") <= 7,  "Medium Stay (4-7 days)")
        .when(col("length_of_stay_days") <= 14, "Long Stay (8-14 days)")
        .otherwise("Extended Stay (15+ days)")
    )

    # 4. Date parts
    .withColumn("admission_year",    year(col("admission_date")))
    .withColumn("admission_month",   month(col("admission_date")))
    .withColumn("admission_quarter", quarter(col("admission_date")))
    .withColumn(
        "admission_day_of_week",
        dayofweek(col("admission_date"))
    )
    .withColumn(
        "is_weekend_admission",
        when(dayofweek(col("admission_date")).isin([1, 7]), "Y")
        .otherwise("N")
    )

    # 5. Combine date + time into timestamp
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

    # 6. Standardise text fields
    .withColumn("admitting_ward",   trim(col("admitting_ward")))
    .withColumn("admission_type",   trim(col("admission_type")))
    .withColumn("facility_name",    trim(col("facility_name")))
    .withColumn("diagnosis",        trim(col("diagnosis")))

    # 7. Readmission flag validation
    .withColumn(
        "is_readmission_validated",
        when(col("is_readmission") == "Y", "Y")
        .otherwise("N")
    )

    # 8. Data quality flag
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

    # 9. Drop raw Bronze metadata
    .drop("record_hash", "is_duplicate_flag", "ingestion_timestamp")
)

silver_adm_df = silver_adm_df.dropDuplicates(
    ["patient_id", "admission_date", "admission_type"]
)

# ============================================================
# CELL 3 : QUARANTINE SPLIT & WRITE
# ============================================================
quarantine_df  = silver_adm_df.filter(col("data_quality_flag").startswith("FAIL"))
clean_df       = silver_adm_df.filter(~col("data_quality_flag").startswith("FAIL"))

print(f"✅ Clean records   : {clean_df.count():,}")
print(f"⚠️  Quarantine records: {quarantine_df.count():,}")

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.quarantine.admission_discharge")
)

(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hospital_analytics.02_silver.admission_discharge")
)

print(f"✅ hospital_analytics.02_silver.admission_discharge written")

