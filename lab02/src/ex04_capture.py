"""Lab 2, Exercise 4: read the job/stage/task counts from the running Spark UI
(its REST API) and take real screenshots of the Jobs tab and one Stage page.

Start src/ex04_job.py first; it writes runs/ex04_ui.ready with the UI address.
Run: ~/bda/venv/bin/python src/ex04_capture.py
Needs: ~/bda/venv/bin/pip install playwright && ~/bda/venv/bin/python -m playwright install chromium
"""
import json
import os
import re
import urllib.request
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
ui = (HERE / "runs" / "ex04_ui.ready").read_text().strip()
port = re.search(r":(\d+)$", ui).group(1)
base = f"http://localhost:{port}"          # the UI may print the WSL address; localhost works inside WSL


def get(path):
    with urllib.request.urlopen(f"{base}/api/v1{path}", timeout=10) as r:
        return json.load(r)


def ts(s):
    return datetime.strptime(s.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z")


app = get("/applications")[0]["id"]
jobs = sorted(get(f"/applications/{app}/jobs"), key=lambda j: j["jobId"])
stages = {s["stageId"]: s for s in get(f"/applications/{app}/stages?status=complete")}
stages.update({s["stageId"]: s for s in get(f"/applications/{app}/stages?status=skipped")})

print()
print(f"  UI {base}   application {app}")
print(f"  number of jobs: {len(jobs)}")
print(f"  {'job':>4}  {'group':<8} {'status':<10}{'duration s':>11}{'stages':>8}{'skipped':>9}{'tasks':>7}  description")
for j in jobs:
    dur = (ts(j["completionTime"]) - ts(j["submissionTime"])).total_seconds()
    j["_dur"] = dur
    print(f"  {j['jobId']:>4}  {j.get('jobGroup', ''):<8} {j['status']:<10}{dur:>11.3f}{len(j['stageIds']):>8}"
          f"{j['numSkippedStages']:>9}{j['numTasks']:>7}  {j.get('description', j['name'])[:60]}")

longest = max(jobs, key=lambda j: j["_dur"])
most = max(jobs, key=lambda j: len(j["stageIds"]))
run_ids = [s for s in sorted(longest["stageIds"]) if stages.get(s, {}).get("status") == "COMPLETE"]
first = run_ids[0]
fs = stages[first]
print()
print(f"  longest job (by duration): job {longest['jobId']}, {longest['_dur']:.3f} s")
print(f"    stages in that job: {len(longest['stageIds'])} (ids {sorted(longest['stageIds'])}), "
      f"of which skipped: {longest['numSkippedStages']}")
print(f"    first stage that ran: stage {first}, {fs['numTasks']} tasks, name: {fs['name'][:70]}")
print(f"    shuffles implied by the stages that ran: {len(run_ids) - 1} (stages that ran - 1)")
print(f"  job with the most stages: job {most['jobId']}, {len(most['stageIds'])} stages, {most['_dur']:.3f} s")
print()

from playwright.sync_api import sync_playwright   # imported late so the numbers print even if it is missing

with sync_playwright() as p:
    # CHROMIUM_PATH lets a pre-installed Chromium be used instead of Playwright's download.
    exe = os.environ.get("CHROMIUM_PATH")
    b = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
    pg = b.new_page(viewport={"width": 1400, "height": 900})
    pg.goto(f"{base}/jobs/", wait_until="networkidle")
    pg.screenshot(path=str(HERE / "figures" / "lab02_ex04_jobs.png"), full_page=True)
    pg.goto(f"{base}/stages/stage/?id={first}&attempt=0", wait_until="networkidle")
    pg.screenshot(path=str(HERE / "figures" / "lab02_ex04_stage.png"), full_page=True)
    b.close()
print("  screenshots: figures/lab02_ex04_jobs.png, figures/lab02_ex04_stage.png")
print()
