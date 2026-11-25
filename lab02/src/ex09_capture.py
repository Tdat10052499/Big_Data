"""Lab 2, Exercise 9: read every stage and task of the job in group "ex09" from the
running Spark UI (REST API) and screenshot the Stage pages with the Summary Metrics table.

Start src/ex09_skew.py first; it writes runs/ex09_ui.ready with the UI address.
Creates runs/ex09_ui.stop at the end so ex09_skew.py stops.
Run: ~/bda/venv/bin/python src/ex09_capture.py
Needs: ~/bda/venv/bin/pip install playwright && ~/bda/venv/bin/python -m playwright install chromium
"""
import json
import os
import re
import urllib.request
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
STOP = HERE / "runs" / "ex09_ui.stop"
ui = (HERE / "runs" / "ex09_ui.ready").read_text().strip()
port = re.search(r":(\d+)$", ui).group(1)
base = f"http://localhost:{port}"


def get(path):
    with urllib.request.urlopen(f"{base}/api/v1{path}", timeout=10) as r:
        return json.load(r)


def ts(s):
    return datetime.strptime(s.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z")


try:
    app = get("/applications")[0]["id"]
    A = f"/applications/{app}"
    jobs = [j for j in get(f"{A}/jobs") if j.get("jobGroup") == "ex09"]
    print()
    print(f"  UI {base}   application {app}")
    print(f"  jobs in group ex09: {[j['jobId'] for j in jobs]}")
    stage_ids = sorted({s for j in jobs for s in j["stageIds"]})
    stages = []
    for sid in stage_ids:
        for st in get(f"{A}/stages/{sid}"):
            if st["status"] == "COMPLETE":
                stages.append(st)
    print()
    print(f"  {'stage':>5}{'attempt':>8}{'tasks':>7}{'duration s':>12}{'input rec':>12}{'shuf read rec':>15}{'shuf write rec':>16}  name")
    for st in stages:
        dur = (ts(st["completionTime"]) - ts(st["submissionTime"])).total_seconds()
        print(f"  {st['stageId']:>5}{st['attemptId']:>8}{st['numTasks']:>7}{dur:>12.3f}{st['inputRecords']:>12,}"
              f"{st['shuffleReadRecords']:>15,}{st['shuffleWriteRecords']:>16,}  {st['name'][:50]}")

    roles = {}
    for st in stages:
        sid, att = st["stageId"], st["attemptId"]
        role = "post-shuffle" if st["shuffleReadRecords"] > 0 else "scan"
        roles[role] = st
        tasks = get(f"{A}/stages/{sid}/{att}/taskList?offset=0&length=10000")
        tasks.sort(key=lambda t: t["index"])
        print()
        print(f"  ---- stage {sid} ({role}), {len(tasks)} tasks ----")
        print(f"  {'index':>6}{'partition':>10}{'duration ms':>13}{'input rec':>12}{'shuf read rec':>15}{'shuf write rec':>16}")
        for t in tasks:
            m = t.get("taskMetrics", {})
            print(f"  {t['index']:>6}{t.get('partitionId', t['index']):>10}{t.get('duration', 0):>13}"
                  f"{m.get('inputMetrics', {}).get('recordsRead', 0):>12,}"
                  f"{m.get('shuffleReadMetrics', {}).get('recordsRead', 0):>15,}"
                  f"{m.get('shuffleWriteMetrics', {}).get('recordsWritten', 0):>16,}")
        slow = max(tasks, key=lambda t: t.get("duration", 0))
        print(f"  slowest task: index {slow['index']}, partition {slow.get('partitionId', slow['index'])}, "
              f"{slow.get('duration', 0)} ms")
        q = get(f"{A}/stages/{sid}/{att}/taskSummary?quantiles=0,0.25,0.5,0.75,1.0")
        field = "duration" if "duration" in q else "executorRunTime"
        vals = q[field]
        print(f"  task {field} quantiles from the UI (taskSummary, ms): "
              f"min {vals[0]:.0f}, 25th {vals[1]:.0f}, median {vals[2]:.0f}, 75th {vals[3]:.0f}, max {vals[4]:.0f}")
    post = roles.get("post-shuffle")
    print()
    print(f"  post-shuffle stage has 8 tasks: {'PASS' if post and post['numTasks'] == 8 else 'FAIL'}"
          f"  ({post['numTasks'] if post else 'no post-shuffle stage'} tasks)")
    print()

    from playwright.sync_api import sync_playwright   # imported late so the numbers print even if it is missing

    shots = {"post-shuffle": "lab02_ex09_task_summary.png", "scan": "lab02_ex09_task_summary_scan.png"}
    with sync_playwright() as p:
        exe = os.environ.get("CHROMIUM_PATH")
        b = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
        pg = b.new_page(viewport={"width": 1400, "height": 900})
        for role, fname in shots.items():
            st = roles[role]
            pg.goto(f"{base}/stages/stage/?id={st['stageId']}&attempt={st['attemptId']}", wait_until="networkidle")
            pg.wait_for_selector("text=Summary Metrics", timeout=20000)
            pg.wait_for_timeout(1500)                      # the table is filled by the page's JavaScript
            pg.get_by_text("Summary Metrics").first.scroll_into_view_if_needed()
            pg.screenshot(path=str(HERE / "figures" / fname), full_page=True)
            print(f"  screenshot: figures/{fname} (stage {st['stageId']}, {role})")
        b.close()
    print()
finally:
    STOP.write_text("stop\n")
