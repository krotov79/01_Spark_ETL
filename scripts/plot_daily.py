import matplotlib
matplotlib.use("Agg")  # headless backend for WSL/servers
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

INP = "data_parquet/aggregates/daily"
OUT = "assets/daily_revenue_tips.png"

spark = (SparkSession.builder
         .appName("NYC_Taxi_Plot_Daily")
         .config("spark.sql.session.timeZone","UTC")
         .getOrCreate())

df = spark.read.parquet(INP)

# Keep only Jan 2023 (in case other dates slipped in)
df = df.filter((col("day") >= "2023-01-01") & (col("day") < "2023-02-01"))

# Small dataset -> convert to pandas for plotting
pdf = (df.orderBy("day")
         .select("day","revenue_total","tips_total")
         .toPandas())

# Guard: empty dataset
if pdf.empty:
    print("No rows found for 2023-01 in aggregates; aborting plot.")
else:
    fig = plt.figure(figsize=(10,5))
    plt.plot(pdf["day"], pdf["revenue_total"], label="Revenue (total)")
    plt.plot(pdf["day"], pdf["tips_total"], label="Tips (total)")
    plt.title("NYC Yellow Taxi — Daily Revenue & Tips (2023-01)")
    plt.xlabel("Day")
    plt.ylabel("Amount (USD)")
    plt.legend()
    plt.tight_layout()
    fig.savefig(OUT, dpi=150)
    print(f"Plot saved to {OUT}")

spark.stop()
