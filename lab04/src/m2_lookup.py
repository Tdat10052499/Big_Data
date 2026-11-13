"""Lab 4, Section 8: what an index changes, read from the query plan and then timed.

Runs with plain Python, not Spark. It needs pymongo, which is in ~/bda/venv.

    python src/m2_lookup.py              # drops any zone_id index first, so it starts clean
"""
import random
import statistics
import time

from pymongo import MongoClient

db = MongoClient("mongodb://127.0.0.1:27017")["bda"]
coll = db["zone_summary"]
QUERY = {"zone_id": 132}


def stages(plan):
    """The winning plan as a chain of stage names, outermost first."""
    plan = plan.get("queryPlan", plan)            # newer servers wrap the tree once more
    chain = []
    while plan:
        chain.append(plan["stage"])
        plan = plan.get("inputStage") or (plan.get("inputStages") or [None])[0]
    return " > ".join(chain)


def explain():
    e = db.command("explain", {"find": "zone_summary", "filter": QUERY},
                   verbosity="executionStats")
    s = e["executionStats"]
    return (stages(e["queryPlanner"]["winningPlan"]), s["nReturned"],
            s["totalKeysExamined"], s["totalDocsExamined"], s["executionTimeMillis"])


def timed(n=1000):
    zones = [d["zone_id"] for d in coll.find({}, {"zone_id": 1})]
    rnd = random.Random(42)
    picks = [rnd.choice(zones) for _ in range(n)]
    coll.find_one({"zone_id": picks[0]})          # warm-up
    runs = []
    for z in picks:
        t0 = time.perf_counter()
        coll.find_one({"zone_id": z})
        runs.append((time.perf_counter() - t0) * 1000)
    runs.sort()
    return statistics.median(runs), runs[int(0.95 * n) - 1]


for name in [i["name"] for i in coll.list_indexes() if i["name"] != "_id_"]:
    coll.drop_index(name)

print()
print(f"  collection bda.zone_summary, {coll.count_documents({}):,} documents, query {QUERY}")
print()
print(f"  {'':19}{'plan':<17}{'returned':>9}{'keys':>7}{'docs':>7}{'ms':>5}")
before = explain()
print(f"  {'no index':19}{before[0]:<17}{before[1]:>9}{before[2]:>7}{before[3]:>7}{before[4]:>5}")
t_before = timed()

coll.create_index("zone_id")
after = explain()
print(f"  {'index on zone_id':19}{after[0]:<17}{after[1]:>9}{after[2]:>7}{after[3]:>7}{after[4]:>5}")
t_after = timed()

print()
print("  1,000 lookups of random zones, one at a time, from Python")
print(f"    no index           median {t_before[0]:.3f} ms   95th percentile {t_before[1]:.3f} ms")
print(f"    index on zone_id   median {t_after[0]:.3f} ms   95th percentile {t_after[1]:.3f} ms")
print()
