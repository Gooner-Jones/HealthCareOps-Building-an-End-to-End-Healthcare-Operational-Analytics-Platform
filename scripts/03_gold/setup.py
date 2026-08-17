# ============================================================
# SETUP
# ============================================================
from pyspark.sql.functions import col, md5, lit, when

spark.sql("CREATE SCHEMA IF NOT EXISTS hospital_analytics.03_gold")
