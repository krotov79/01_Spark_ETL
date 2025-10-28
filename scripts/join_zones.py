from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, to_timestamp, year, month, expr, broadcast

INP = "data_parquet/yellow"              # from Step 2
LOOKUP = "data_raw/taxi_zone_lookup.csv" # downloaded above
OUT = "data_parquet/yellow_zones"

spark = (SparkSession.builder
         .appName("NYC_Taxi_Join_Zones")
         .config("spark.sql.session.timeZone","UTC")
         .getOrCreate())

# --- Trips (already cleaned & featured) ---
trips = spark.read.parquet(INP)

# --- Zone lookup (small, perfect for broadcast) ---
zones = (spark.read.option("header", True)
                   .csv(LOOKUP)
                   .selectExpr("cast(LocationID as int) as LocationID",
                               "Borough","Zone","service_zone"))

# Prep two copies for PU and DO
pu = (zones.withColumnRenamed("LocationID","PULocationID")
           .withColumnRenamed("Borough","PU_Borough")
           .withColumnRenamed("Zone","PU_Zone")
           .withColumnRenamed("service_zone","PU_service_zone"))

do = (zones.withColumnRenamed("LocationID","DOLocationID")
           .withColumnRenamed("Borough","DO_Borough")
           .withColumnRenamed("Zone","DO_Zone")
           .withColumnRenamed("service_zone","DO_service_zone"))

# --- Join (left to retain all trips) ---
joined = (trips
          .join(broadcast(pu), on="PULocationID", how="left")
          .join(broadcast(do), on="DOLocationID", how="left"))

# --- (Optional) Keep only 2023-01 to avoid weird legacy timestamps ---
joined = joined.filter(
    (col("pickup_ts") >= to_timestamp(lit("2023-01-01 00:00:00"))) &
    (col("pickup_ts") <  to_timestamp(lit("2023-02-01 00:00:00")))
)

# --- Basic Data Quality checks ---
n_trips = trips.count()
n_joined = joined.count()
print(f"Row count — trips: {n_trips:,}  |  after join (filtered to 2023-01): {n_joined:,}")

null_pu = joined.filter(col("PU_Zone").isNull()).count()
null_do = joined.filter(col("DO_Zone").isNull()).count()
print(f"Null zones — PU_Zone: {null_pu:,}  |  DO_Zone: {null_do:,}")

# Quick sanity: top 5 rows with borough/zone names
joined.select("pickup_ts","PULocationID","PU_Borough","PU_Zone",
              "DOLocationID","DO_Borough","DO_Zone","total_amount","tip_amount") \
      .orderBy("pickup_ts") \
      .show(5, truncate=False)

# --- Write (partition by year/month for pruning) ---
joined = (joined
          .withColumn("pickup_year", year(col("pickup_ts")))
          .withColumn("pickup_month", month(col("pickup_ts"))))

(joined
 .repartition(4)
 .write
 .mode("overwrite")
 .partitionBy("pickup_year","pickup_month")
 .parquet(OUT))

spark.stop()
print(f"Join ✅  Wrote enriched trips to {OUT}")
