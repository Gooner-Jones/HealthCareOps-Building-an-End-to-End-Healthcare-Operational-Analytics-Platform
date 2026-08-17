# ============================================================
# SETUP & READ TODAY'S INCREMENTAL BRONZE RECORDS
# ============================================================
from pyspark.sql.functions import (
    col, lit, current_date, when,
    trim, upper, length, regexp_replace, rand
)
from delta.tables import DeltaTable

bronze_table = "hospital_analytics.01_bronze.patient_info"
silver_table = "hospital_analytics.02_silver.patient_info"

load_date = spark.sql("SELECT current_date() as current_date").collect()[0]["current_date"]

inc_raw_df = (
    spark.read.format("delta").table(bronze_table)
    .filter(col("ingestion_date") == lit(load_date))
)
print(f"📥 Incremental Bronze records for {load_date}: {inc_raw_df.count():,}")

# Add province columns to Silver if missing (safe to re-run)
try:
    spark.sql(f"ALTER TABLE {silver_table} ADD COLUMN province STRING")
    spark.sql(f"ALTER TABLE {silver_table} ADD COLUMN province_name STRING")
    print("✅ province and province_name columns added to patient_info Silver")
except Exception:
    print("ℹ️  province columns already exist — skipping.")

# ============================================================
# SILVER TRANSFORMATION
# (fixed: marital status correction now uses a per-row rand()
# column instead of a single Python random.choices() call)
# ============================================================
transformed_df = (
    inc_raw_df
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

    .withColumn(
        "gender",
        when(upper(col("gender")) == "MALE",    "Male")
        .when(upper(col("gender")) == "FEMALE", "Female")
        .otherwise("Unknown")
    )

    .withColumn(
        "age_band",
        when(col("age") <= 12,  "Child (0-12)")
        .when(col("age") <= 17, "Adolescent (13-17)")
        .when(col("age") <= 35, "Young Adult (18-35)")
        .when(col("age") <= 59, "Middle Aged (36-59)")
        .when(col("age") <= 74, "Senior (60-74)")
        .otherwise("Elderly (75+)")
    )

    .withColumn("_rand_marital", rand())
    .withColumn(
        "marital_status",
        when(col("age") < 18, "Single")
        .when(
            (col("age") < 23) & (col("marital_status").isin(["Widowed", "Divorced"])),
            "Single"
        )
        .when(
            (col("age") < 35) & (col("marital_status") == "Widowed"),
            when(col("_rand_marital") < 0.50, "Single")
            .when(col("_rand_marital") < 0.90, "Married")
            .otherwise("Divorced")
        )
        .otherwise(col("marital_status"))
    )
    .drop("_rand_marital")

    .withColumn(
        "funding_category",
        when(col("payment_type") == "Government (Public)", "Public")
        .when(col("payment_type").isin(["Medical Aid", "Private (Self-pay)"]), "Private")
        .when(col("payment_type") == "RAF", "RAF")
        .otherwise("Uninsured / Other")
    )

    .withColumn(
        "province_name",
        when(col("province") == "GP",  "Gauteng")
        .when(col("province") == "LP",  "Limpopo")
        .when(col("province") == "MP",  "Mpumalanga")
        .when(col("province") == "NW",  "North West")
        .when(col("province") == "FS",  "Free State")
        .when(col("province") == "KZN", "KwaZulu-Natal")
        .when(col("province") == "WC",  "Western Cape")
        .when(col("province") == "EC",  "Eastern Cape")
        .when(col("province") == "NC",  "Northern Cape")
        .otherwise("Unknown")
    )

    .withColumn(
        "has_comorbidity",
        when(col("secondary_diagnosis").isNotNull(), "Y").otherwise("N")
    )
    .withColumn(
        "high_risk_comorbidity_flag",
        when(
            col("primary_diagnosis").isin(["HIV/AIDS", "Tuberculosis (TB)"]) |
            col("secondary_diagnosis").isin(["HIV/AIDS", "Tuberculosis (TB)", "Type 2 Diabetes Mellitus"]),
            "Y"
        ).otherwise("N")
    )

    .withColumn(
        "data_quality_flag",
        when(col("patient_id").isNull(), "FAIL - Missing Patient ID")
        .when(col("first_name").isNull() | col("last_name").isNull(), "FAIL - Missing Name")
        .when((col("age") < 0) | (col("age") > 120), "FAIL - Invalid Age")
        .when(length(regexp_replace(col("id_number"), "[^0-9]", "")) != 13, "WARN - Invalid ID Number")
        .when(col("gender") == "Unknown", "WARN - Unknown Gender")
        .otherwise("PASS")
    )

    .withColumn("effective_start_date", current_date())
    .withColumn("effective_end_date",   lit(None).cast("date"))
    .withColumn("is_current",           lit("Y"))

    .drop("record_hash", "is_duplicate_flag", "ingestion_timestamp")
)

# ============================================================
# SCHEMA ALIGNMENT CHECK
# ============================================================
silver_cols   = set(spark.read.format("delta").table(silver_table).columns)
incoming_cols = set(transformed_df.columns)

missing_in_silver = incoming_cols - silver_cols
missing_in_data   = silver_cols   - incoming_cols

if missing_in_silver:
    print(f"⚠️  Columns in data but NOT in Silver : {missing_in_silver}")
    raise Exception("Schema mismatch — fix before writing to Silver.")
if missing_in_data:
    print(f"⚠️  Columns in Silver but NOT in data : {missing_in_data}")
    raise Exception("Schema mismatch — fix before writing to Silver.")

print("✅ Schemas aligned — safe to proceed.")

# ============================================================
# SCD TYPE 2 MERGE
# ============================================================
silver_df = (
    spark.read.format("delta").table(silver_table)
    .filter(col("is_current") == "Y")
)

updates_df = (
    transformed_df.alias("bronze")
    .join(silver_df.alias("silver"), on="patient_id", how="inner")
    .filter(
        (col("silver.age")               != col("bronze.age"))               |
        (col("silver.primary_diagnosis") != col("bronze.primary_diagnosis")) |
        (col("silver.marital_status")    != col("bronze.marital_status"))    |
        (col("silver.last_visit_date")   != col("bronze.last_visit_date"))
    )
    .select("bronze.*")
)
update_count = updates_df.count()
print(f"🔄 Records to update  : {update_count:,}")

new_df = transformed_df.alias("bronze").join(silver_df.alias("silver"), on="patient_id", how="left_anti")
insert_count = new_df.count()
print(f"➕ New records to insert: {insert_count:,}")

if update_count > 0:
    update_ids = [row["patient_id"] for row in updates_df.select("patient_id").distinct().collect()]
    update_ids_list = ", ".join([f"'{pid}'" for pid in update_ids])
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.update(
        condition=f"patient_id IN ({update_ids_list}) AND is_current = 'Y'",
        set={"effective_end_date": "current_date()", "is_current": "'N'"}
    )
    print(f"✅ Expired {update_count:,} Silver records for updated patients.")
else:
    print("ℹ️  No existing Silver records to expire.")

new_versions_df = updates_df.union(new_df)
total_inserts = new_versions_df.count()

if total_inserts > 0:
    new_versions_df.write.format("delta").mode("append").saveAsTable(silver_table)
    print(f"✅ Inserted {total_inserts:,} records into {silver_table}.")
else:
    print("ℹ️  No new records to insert into Silver.")

print(f"\n✅ SCD2 merge complete | {update_count:,} updated + {insert_count:,} new patients | {load_date}")

