# ============================================================
# CELL 1 : Create a text widget for catalog name and retrieve its value
# ============================================================
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType
) 

dbutils.widgets.text("catalog_name", "hospital_analytics", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")



spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.01_bronze")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA 01_bronze")
