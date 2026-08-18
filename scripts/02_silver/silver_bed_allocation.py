# ============================================================
# CELL 1 : SETUP & READ BRONZE
# ============================================================
from pyspark.sql.functions import (
    col, when, trim, datediff, current_date
)

bronze_bed_df = spark.read.format("delta").table("hospital_analytics.01_bronze.bed_allocation")

# ============================================================
# CELL 2 : QUARANTINE SPLIT & WRITE
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
