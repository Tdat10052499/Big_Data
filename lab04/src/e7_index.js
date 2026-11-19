// Lab 4, Exercise 7: where the index pays, on bda.trips_jan (2,724,143 documents).
// Run:  mongosh bda --quiet src/e7_index.js
// For each query and each index state it prints nReturned, totalKeysExamined,
// totalDocsExamined and executionTimeMillis. Course timing rule: one warm-up explain,
// then three scored explains; all three times and the best are printed.
const c = db.trips_jan;
const D = ISODate("2024-01-15");
const queries = [
  ["Q-a zone 132",              {PULocationID: 132}],
  ["Q-b zone 132 on 15 Jan",    {PULocationID: 132, pickup_date: D}],
  ["Q-c all trips on 15 Jan",   {pickup_date: D}],
];

function plan(p) {
  p = p.queryPlan || p;
  const out = [];
  while (p) { out.push(p.stage + (p.indexName ? "(" + p.indexName + ")" : ""));
              p = p.inputStage || (p.inputStages || [])[0]; }
  return out.join(" > ");
}

function measure(label) {
  print("");
  print("== " + label + "   indexes: " + c.getIndexes().map(i => i.name).join(", "));
  for (const [name, q] of queries) {
    c.find(q).explain("executionStats");                     // warm-up, not scored
    const runs = [];
    let s, wp;
    for (let i = 0; i < 3; i++) {
      const e = c.find(q).explain("executionStats");
      s = e.executionStats; wp = e.queryPlanner.winningPlan;
      runs.push(s.executionTimeMillis);
    }
    print(`  ${name.padEnd(26)} nReturned ${String(s.nReturned).padStart(8)}  keys ${String(s.totalKeysExamined).padStart(8)}` +
          `  docs ${String(s.totalDocsExamined).padStart(8)}  ms runs ${runs.join(" / ")} best ${Math.min(...runs)}  plan ${plan(wp)}`);
  }
}

// start clean: only _id
c.getIndexes().filter(i => i.name !== "_id_").forEach(i => c.dropIndex(i.name));
print("documents in bda.trips_jan: " + c.countDocuments({}));

measure("1. no index but _id");

c.createIndex({PULocationID: 1, pickup_date: 1});
measure("2. after createIndex({PULocationID: 1, pickup_date: 1})");

c.createIndex({pickup_date: 1});
measure("3. after adding createIndex({pickup_date: 1}) for the third query");

print("");
print("== index sizes, db.trips_jan.stats().indexSizes (bytes)");
printjson(c.stats().indexSizes);
