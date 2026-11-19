# New laptop setup for Lab 4 (WSL Ubuntu 24.04)

The old machine (DESKTOP-ANNRGGL) held the cluster for Labs 1-2. This folder sets up the same stack on a new
Windows laptop. Steps A-C are typed by the student; step D is a prompt for Claude Code.

## A. Windows PowerShell, "Run as administrator"
```powershell
wsl --install -d Ubuntu-24.04
```
Restart Windows if asked. When Ubuntu opens, create the Linux user **`tdat1`** (it becomes the HDFS user; any other name breaks every `/user/tdat1` path).

## B. Ubuntu terminal
```bash
sudo apt-get update && sudo apt-get install -y git
git clone https://github.com/Tdat10052499/Big_Data.git ~/Big_Data
cd ~/Big_Data && git checkout claude/lab04-storage-b5z7je
sudo bash lab04/setup_laptop/01_root.sh     # Java, MongoDB 8.0, jq, gh, Hadoop 3.4.3, Spark 3.5.9 (about 1.5 GB download)
bash lab04/setup_laptop/02_user.sh          # configs, ~/bda/venv, bda-start.sh, format and start HDFS
```
Both must end with `... done.` If one prints STOP, copy its output to the project thread.

## C. A new Ubuntu terminal
```bash
curl -fsSL https://claude.ai/install.sh | bash
gh auth login                  # GitHub.com, HTTPS, log in with a browser
gh auth setup-git
git config --global user.name "Ho Du Tuan Dat"
git config --global user.email "<your GitHub email>"
```

## D. Claude Code
```bash
cd ~/Big_Data && claude
```
Paste `lab04/prompts/p0_laptop_rebuild.md` (below its line). It rebuilds `raw`, `ref`, `curated`, `curated_q1` and `agg`
with the Lab 1-2 scripts, checks the row counts against the old machine, then runs P1.

## Differences from the old machine (for the run record)
- Ubuntu 24.04 (the manual's version) instead of 26.04; Python is Ubuntu's 3.12 instead of 3.14.4.
- `bda-start.sh`, `bda-status.sh`, `bda-stop.sh` are the student's own (`setup_laptop/bin/`), because the lecturer's are not in the repo. They start HDFS with `hdfs --daemon` (no ssh, no YARN) and MongoDB with `mongod --fork` on `/data/db`, bound to 127.0.0.1.
- Lab 3 data (`stream/ck/restart`) is not rebuilt; only manual section 7 reads it.
