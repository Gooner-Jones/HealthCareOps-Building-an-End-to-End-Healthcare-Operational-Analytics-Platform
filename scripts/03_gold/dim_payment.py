# ============================================================
# CELL 1 : dim_payment
# ============================================================
from pyspark.sql.functions import col, md5, when

PAYMENT_TYPES = ["Medical Aid", "Private (Self-pay)", "Government (Public)", "RAF", "Uninsured"]

payment_rows = [(p,) for p in PAYMENT_TYPES]

dim_payment = spark.createDataFrame(payment_rows, schema=["payment_type"])

dim_payment = (
    dim_payment
    .withColumn(
        "funding_category",
        when(col("payment_type") == "Government (Public)", "Public")
        .when(col("payment_type").isin(["Medical Aid", "Private (Self-pay)"]), "Private")
        .when(col("payment_type") == "RAF", "RAF")
        .otherwise("Uninsured / Other")
    )
    .withColumn("payment_sk", md5(col("payment_type")))
    .select("payment_sk", "payment_type", "funding_category")
)

dim_payment.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.dim_payment")

print(f"✅ dim_payment written: {dim_payment.count()} payment types")
dim_payment.show(truncate=False)

# ============================================================
# CELL 2 : VALIDATE dim_payment against patient_info Silver
# ============================================================
dim_payment_check = spark.read.table("hospital_analytics.03_gold.dim_payment")

pat_df = spark.read.table("hospital_analytics.02_silver.patient_info")
pat_unmatched = (
    pat_df.withColumn("payment_sk", md5(col("payment_type")))
    .join(dim_payment_check, "payment_sk", "left_anti")
)
print(f"patient_info unmatched payment types: {pat_unmatched.count()}")
