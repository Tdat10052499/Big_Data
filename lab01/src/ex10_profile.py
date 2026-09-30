"""Exercise 10 (and Section 8.3): pickup zones joined to names, the hourly pattern, robustness checks."""
import csv, os
from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

spark = SparkSession.builder.appName("lab01-ex10").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
H = "hdfs://localhost:9000/user/tdat1/nyc"
cur = spark.read.parquet(f"{H}/curated")
raw = {m: spark.read.parquet(f"{H}/raw/yellow_tripdata_2024-0{m}.parquet") for m in (1, 2, 3)}
zones = (spark.read.option("header", True).csv(f"{H}/ref/taxi_zone_lookup.csv")
         .withColumn("LocationID", F.col("LocationID").cast("int")))
zname = {r["LocationID"]: (r["Borough"], r["Zone"]) for r in zones.collect()}
NC = cur.count()
NR = raw[1].count()

print("\n=== Section 8.3: zone lookup ===")
print(f"zone rows       {zones.count()}")
print(f"zones in lookup {zones.select('LocationID').distinct().count()}")
print(f"zones used (January raw pickups)     {raw[1].select('PULocationID').distinct().count()}")
print(f"zones used (January curated pickups) {cur.select('PULocationID').distinct().count()}")
print("names of zones 264 and 265:", [(i, zname.get(i)) for i in (264, 265)])
unmatched = cur.join(zones, cur.PULocationID == zones.LocationID, "left_anti").count()
print(f"curated rows whose PULocationID has no name in the lookup: {unmatched:,}")

cur_cnt = {r["PULocationID"]: r["count"] for r in cur.groupBy("PULocationID").count().collect()}
raw_cnt = {r["PULocationID"]: r["count"] for r in raw[1].groupBy("PULocationID").count().collect()}
ranked = sorted(cur_cnt.items(), key=lambda kv: (-kv[1], kv[0]))

print(f"\n=== Exercise 10: top pickup zones, curated January cohort = {NC:,} trips ===")
rows_out = []
for rank, (zid, n) in enumerate(ranked[:10], 1):
    b, z = zname.get(zid, (None, None))
    rw = raw_cnt.get(zid, 0)
    rows_out.append([rank, zid, b, z, n, f"{n / NC:.3%}", rw, f"{1 - n / rw:.3%}"])
    print(f"{rank:>2}. {zid:>3} {str(z):<36} {str(b):<14} {n:>8,}  {n / NC:6.3%} of {NC:,} curated trips; "
          f"raw {rw:,}, dropped {1 - n / rw:.3%} of this zone's {rw:,} raw January rows")
os.makedirs(os.path.expanduser("~/bda/lab01/out"), exist_ok=True)
with open(os.path.expanduser("~/bda/lab01/out/ex10_top_zones.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["rank", "PULocationID", "Borough", "Zone", "curated_trips", "share_of_2724143_curated_trips",
                "raw_trips_in_zone", "share_of_zone_raw_rows_dropped"])
    w.writerows(rows_out)

print(f"\n=== Trips by pickup hour (curated January, {NC:,} trips) ===")
hours = {r["pickup_hour"]: r["count"] for r in cur.groupBy("pickup_hour").count().collect()}
hcounts = [hours.get(h, 0) for h in range(24)]
for h in range(24):
    print(f"  hour {h:>2}: {hcounts[h]:>8,}  {hcounts[h] / NC:6.3%} of {NC:,}")
ph = max(range(24), key=lambda h: hcounts[h])
print(f"peak hour {ph} with {hcounts[ph]:,} trips ({hcounts[ph] / NC:.3%} of {NC:,})")
with open(os.path.expanduser("~/bda/lab01/out/ex10_hourly.csv"), "w", newline="") as f:
    w = csv.writer(f); w.writerow(["pickup_hour", "curated_trips"]); w.writerows(zip(range(24), hcounts))

print("\n=== Checks for the limitation ===")
unk = cur_cnt.get(264, 0) + cur_cnt.get(265, 0)
print(f"(a) curated trips with pickup zone 264 or 265: {unk:,} = {unk / NC:.3%} of the {NC:,} curated trips")
raw_ranked = sorted(raw_cnt.items(), key=lambda kv: (-kv[1], kv[0]))
print(f"(b) top 5 zone ids, curated:  {[z for z, _ in ranked[:5]]}")
print(f"    top 5 zone ids, January raw (no curation): {[z for z, _ in raw_ranked[:5]]}")
print(f"    same ids in the same order: {[z for z, _ in ranked[:5]] == [z for z, _ in raw_ranked[:5]]}")
for m, name in ((2, "February"), (3, "March")):
    cnt = {r["PULocationID"]: r["count"] for r in raw[m].groupBy("PULocationID").count().collect()}
    top = sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    print(f"(c) top 5 zone ids, {name} raw (no curation, cohort = that month's raw rows): {[z for z, _ in top]}; "
          f"same set as January curated top 5: {set(z for z, _ in top) == set(z for z, _ in ranked[:5])}")

# One chart: trips by pickup hour (single series, one colour, no legend needed)
SURFACE, INK, INK2, SERIES, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#2a78d6", "#e4e3de"
fig, ax = plt.subplots(figsize=(9, 4.5), dpi=150, facecolor=SURFACE)
ax.set_facecolor(SURFACE)
ax.bar(range(24), hcounts, width=0.72, color=SERIES, linewidth=0)
ax.set_axisbelow(True)
ax.yaxis.grid(True, color=GRID, linewidth=0.8)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
for s in ("left", "bottom"):
    ax.spines[s].set_color("#c9c8c2")
ax.set_xticks(range(24))
ax.tick_params(colors=INK2, labelsize=9, length=0)
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:,.0f}"))
ax.set_xlabel("Pickup hour (0 = midnight)", color=INK2, fontsize=10)
ax.set_ylabel("Trips (thousands)", color=INK2, fontsize=10)
ax.set_title(f"Yellow taxi trips by pickup hour, January 2024 (curated cohort: {NC:,} trips)",
             loc="left", color=INK, fontsize=11)
ax.annotate(f"Peak: hour {ph}, {hcounts[ph]:,} trips", xy=(ph, hcounts[ph]), xytext=(ph - 9, hcounts[ph] * 1.02),
            color=INK, fontsize=9, arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))
fig.tight_layout()
out = os.path.expanduser("~/bda/lab01/figures/ex10_hourly_trips.png")
fig.savefig(out, facecolor=SURFACE)
print(f"\nchart written: {out}")
spark.stop()
