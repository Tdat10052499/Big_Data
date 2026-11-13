"""Lab 4, Section 8: load the Lab 2 zone summary into MongoDB, and build a second
collection whose documents are shaped for the question a web page asks.

    $SS src/m1_load.py

Writes two collections in the database bda, replacing them if they exist:
  zone_summary   one document per zone, exactly the 258 rows Lab 2 wrote
  zone_profile   one document per zone, with its name and its 24 hourly fares inside it
"""
from pyspark.sql import functions as F
from l0_common import spark_session, NYC, AGG

spark = spark_session("lab04-mongo-load")


def save(df, collection):
    (df.write.format("mongodb").mode("overwrite")
       .option("database", "bda").option("collection", collection).save())


def load(collection):
    return (spark.read.format("mongodb")
            .option("database", "bda").option("collection", collection).load())


# --- 1. the Lab 2 artefact, as it is ------------------------------------------
summary = spark.read.parquet(f"{AGG}/zone_summary")
save(summary, "zone_summary")
back = load("zone_summary")

print()
print(f"  zone_summary   written {summary.count():>4,}   read back {back.count():>4,}")
print(f"  columns before  {', '.join(summary.columns)}")
print(f"  columns after   {', '.join(back.columns)}")
print()
print("  the three busiest zones, read back from MongoDB")
(back.orderBy(F.desc("trips"))
     .select("zone_id", "trips", F.round("avg_fare", 2).alias("avg_fare"),
             F.round("avg_distance", 2).alias("avg_distance"))
     .limit(3).show())

# --- 2. one document per zone, holding everything a page about the zone shows ---
zones = (spark.read.option("header", True).csv(f"{NYC}/ref/taxi_zone_lookup.csv")
         .select(F.col("LocationID").cast("int").alias("zone_id"),
                 F.col("Borough").alias("borough"), F.col("Zone").alias("zone")))
hours = (spark.read.parquet(f"{AGG}/zone_hour")
         .groupBy(F.col("PULocationID").alias("zone_id"))
         .agg(F.sort_array(F.collect_list(F.struct(
             F.col("pickup_hour").alias("hour"),
             F.round("zone_hour_avg", 2).alias("avg_fare")))).alias("hours")))
profile = (summary.join(zones, "zone_id", "left")
                  .join(hours, "zone_id", "left")
                  .select("zone_id", "zone", "borough", "trips",
                          F.round("avg_fare", 2).alias("avg_fare"),
                          F.round("avg_distance", 2).alias("avg_distance"), "hours"))
save(profile, "zone_profile")
print(f"  zone_profile   written {profile.count():>4,}   read back {load('zone_profile').count():>4,}\n")
spark.stop()
