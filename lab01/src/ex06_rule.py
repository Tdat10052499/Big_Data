"""Exercise 6: keep rows with null passenger_count instead of dropping them."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab01-ex06").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")
feb = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-02.parquet")
mar = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-03.parquet")
TOTAL = jan.count()
pc = F.col("passenger_count")

def base(lo, hi):
    return ((F.col("fare_amount") >= 0) & (F.col("trip_distance") > 0)
            & (F.col("tpep_pickup_datetime") >= lo) & (F.col("tpep_pickup_datetime") < hi)
            & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")))

def stats(df, cond):
    r = df.filter(cond).agg(F.count("*").alias("n"), F.avg("fare_amount").alias("mean"),
                            F.stddev("fare_amount").alias("sd")).collect()[0]
    return r["n"], r["mean"], r["sd"]

b_jan = base("2024-01-01", "2024-02-01")
na, ma, sa = stats(jan, b_jan & pc.isNotNull() & (pc > 0))          # rule A (Lab 1)
nb, mb, sb = stats(jan, b_jan & (pc.isNull() | (pc > 0)))           # rule B (keep nulls)
nx, mx, sx = stats(jan, b_jan & pc.isNull())                        # the rows B adds

print(f"\nCohort: the {TOTAL:,} January rows")
print(f"Rule A (drop null passenger_count, Lab 1): rows {na:,}  mean fare_amount {ma:.4f}  sd {sa:.4f}")
print(f"Rule B (keep null passenger_count):        rows {nb:,}  mean fare_amount {mb:.4f}  sd {sb:.4f}")
print(f"Matches the Lab 1 curated count (2,724,143): {na == 2724143}")
print(f"Rows added by rule B: {nb - na:,}  (= rows passing the other four rules with null passenger_count: {nx:,}; consistent: {nb - na == nx})")
print(f"  as a share of rule A rows: {(nb - na) / na:.3%};  as a share of the {TOTAL:,} January rows: {(nb - na) / TOTAL:.3%}")
print(f"Mean fare_amount of the added rows: {mx:.4f}  (sd {sx:.4f})")
d = mb - ma
print(f"Mean difference B - A: {d:+.4f} USD = {d / ma:+.3%} of the rule A mean = {d / sa:+.4f} x the rule A standard deviation")

print("\nYardstick: month-to-month variation of mean fare_amount under rule A, each month's own cohort")
for name, df, lo, hi in [("January", jan, "2024-01-01", "2024-02-01"),
                         ("February", feb, "2024-02-01", "2024-03-01"),
                         ("March", mar, "2024-03-01", "2024-04-01")]:
    n, m, s = stats(df, base(lo, hi) & pc.isNotNull() & (pc > 0))
    print(f"  {name:<9} kept rows {n:,}  mean fare_amount {m:.4f}")
spark.stop()
