from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc, sum as _sum, col, to_timestamp, lit

INP = "data_parquet/yellow"
OUT = "data_parquet/aggregates/daily"

spark = SparkSession.builder.appName("NYC_Taxi_Aggs").getOrCreate()
df = spark.read.parquet(INP)

# Restrict to 2023-01 only (guards against dirty timestamps)
df = df.filter(
    (col("pickup_ts") >= to_timestamp(lit("2023-01-01 00:00:00"))) &
    (col("pickup_ts") <  to_timestamp(lit("2023-02-01 00:00:00")))
)

daily = (
    df.withColumn("day", date_trunc("day", col("pickup_ts")))
      .groupBy("day")
      .agg(
          _sum("tip_amount").alias("tips_total"),
          _sum("total_amount").alias("revenue_total")
      )
      .orderBy("day")
)

daily.show(10, truncate=False)
daily.coalesce(1).write.mode("overwrite").parquet(OUT)

spark.stop()
print(f"Aggregations ✅  Wrote to {OUT}")
