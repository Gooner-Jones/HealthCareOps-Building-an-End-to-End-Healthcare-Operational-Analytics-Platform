# ============================================================
# CELL 1 : dim_date — standard date dimension
# Spans 5 years back to 1 year forward from today, covers
# all admission/discharge/assignment dates comfortably.
# ============================================================
from pyspark.sql.functions import (
    explode, sequence, to_date, lit, year, month, quarter,
    dayofmonth, dayofweek, dayofyear, weekofyear, date_format, when, col
)

date_range_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2020-01-01'),
        to_date('2027-12-31'),
        interval 1 day
    )) AS full_date
""")

dim_date = (
    date_range_df
    .withColumn("date_sk", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))          # 1=Sunday, 7=Saturday
    .withColumn("day_name", date_format(col("full_date"), "EEEE"))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn(
        "is_weekend",
        when(dayofweek(col("full_date")).isin([1, 7]), "Y").otherwise("N")
    )
    .select(
        "date_sk", "full_date", "year", "month", "month_name", "quarter",
        "day_of_month", "day_of_week", "day_name", "week_of_year", "is_weekend"
    )
)

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("hospital_analytics.03_gold.dim_date")

print(f"✅ dim_date written: {dim_date.count():,} days")
dim_date.show(5)
