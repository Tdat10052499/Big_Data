# Lab 4: Storage and Serving Architecture

The manual uses `~/bda/lab04`. Link it to the repo once:

```bash
cd ~/Big_Data && git pull && bash lab04/setup_lab04.sh
```

Each new WSL session:

```bash
bash ~/bda/lab04/start_cluster.sh     # HDFS + MongoDB, prints STOP if either is down
source ~/bda/lab04/env.sh             # cd, venv, $SS, $T
```

Claude Code on the student machine: rules in `CLAUDE.md`, one prompt per step in `prompts/`.

Scripts in `src/` are the lecturer's eleven files with `/user/thaianh/` replaced by `/user/tdat1/`,
plus `log_to_png.py` (copied from Lab 2). The only other change: `best_of_three` in `l0_common.py`
also prints the warm-up and all three scored runs (`[timing]` lines). See `PLAN.md`.
