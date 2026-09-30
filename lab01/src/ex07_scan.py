"""Exercise 7: scan candidate defects in January that Table 8 does not list."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab01-ex07").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")
TOTAL = jan.count()
c = F.col
zero = F.lit(0.0)
parts = (c("fare_amount") + c("extra") + c("mta_tax") + c("tip_amount") + c("tolls_amount")
         + c("improvement_surcharge") + F.coalesce(c("congestion_surcharge"), zero)
         + F.coalesce(c("Airport_fee"), zero))
dur = c("tpep_dropoff_datetime").cast("timestamp").cast("long") - c("tpep_pickup_datetime").cast("timestamp").cast("long")
pos = (dur > 0) & (c("trip_distance") > 0)
ALL = ("all January rows", None)

checks = [  # (name, condition, (cohort name, cohort condition or None))
    ("tip_amount below zero",            c("tip_amount") < 0, ALL),
    ("tolls_amount below zero",          c("tolls_amount") < 0, ALL),
    ("extra below zero",                 c("extra") < 0, ALL),
    ("mta_tax below zero",               c("mta_tax") < 0, ALL),
    ("improvement_surcharge below zero", c("improvement_surcharge") < 0, ALL),
    ("congestion_surcharge below zero",  c("congestion_surcharge") < 0, ("rows with non-null congestion_surcharge", c("congestion_surcharge").isNotNull())),
    ("Airport_fee below zero",           c("Airport_fee") < 0, ("rows with non-null Airport_fee", c("Airport_fee").isNotNull())),
    ("total_amount differs from sum of its components by more than 0.01",
                                         F.abs(c("total_amount") - parts) > 0.01, ALL),
    ("RatecodeID = 99",                  c("RatecodeID") == 99, ("rows with non-null RatecodeID", c("RatecodeID").isNotNull())),
    ("RatecodeID not in 1-6 or 99",      ~c("RatecodeID").isin(1, 2, 3, 4, 5, 6, 99), ("rows with non-null RatecodeID", c("RatecodeID").isNotNull())),
    ("store_and_fwd_flag not Y or N",    ~c("store_and_fwd_flag").isin("Y", "N"), ("rows with non-null store_and_fwd_flag", c("store_and_fwd_flag").isNotNull())),
    ("PULocationID outside 1-265",       (c("PULocationID") < 1) | (c("PULocationID") > 265), ALL),
    ("DOLocationID outside 1-265",       (c("DOLocationID") < 1) | (c("DOLocationID") > 265), ALL),
    ("trip duration above 24 hours",     dur > 86400, ("rows with dropoff after pickup", dur > 0)),
    ("implied speed above 100 mph",      c("trip_distance") / (dur / 3600.0) > 100, ("rows with distance > 0 and dropoff after pickup", pos)),
    ("tip_amount > 0 on cash trips (payment_type = 2)", (c("payment_type") == 2) & (c("tip_amount") > 0), ("rows with payment_type = 2", c("payment_type") == 2)),
]

aggs = []
for i, (_, cond, (_, coh)) in enumerate(checks):
    aggs.append(F.count(F.when(cond, 1)).alias(f"n{i}"))
    aggs.append((F.count(F.when(coh, 1)) if coh is not None else F.count(F.lit(1))).alias(f"d{i}"))
row = jan.agg(*aggs).collect()[0].asDict()

print(f"\nJanuary rows: {TOTAL:,}. Every share names its own cohort.\n")
for i, (name, _, (cname, _)) in enumerate(checks):
    n, d = row[f"n{i}"], row[f"d{i}"]
    print(f"{name:<70} {n:>9,}  {n / d if d else 0:8.3%} of {cname} ({d:,})")

print("\nVendorID values:");          jan.groupBy("VendorID").count().orderBy("VendorID").show()
print("RatecodeID values:");        jan.groupBy("RatecodeID").count().orderBy("RatecodeID").show()
print("store_and_fwd_flag values:"); jan.groupBy("store_and_fwd_flag").count().orderBy("store_and_fwd_flag").show()

print("Sample rows where total_amount differs from the sum of its components:")
(jan.filter(F.abs(c("total_amount") - parts) > 0.01)
    .select("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
            "congestion_surcharge", "Airport_fee", "total_amount", (c("total_amount") - parts).alias("diff"))
    .limit(5).show(truncate=False))
spark.stop()
