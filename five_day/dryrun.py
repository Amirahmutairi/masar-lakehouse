"""Trainer tool: run the bash blocks of DAY1..DAY5.md in order in ONE shell, as a trainee would.

Usage (repo root, in the Codespace):  python /tmp/fd/five_day/dryrun.py /tmp/fd/five_day
Skips Kafka/Docker, git and the trainer-only late-trip drill. Writes /tmp/dry/summary.txt.
"""
import os, re, shutil, subprocess, sys, time

FD = sys.argv[1]
DEST = {
 "_masar__sources.yml": "dbt/masar/models/staging/masar", "stg_trips.sql": "dbt/masar/models/staging/masar",
 "stg_drivers.sql": "dbt/masar/models/staging/masar", "silver_trips.sql": "dbt/masar/models/marts/silver",
 "_silver__models.yml": "dbt/masar/models/marts/silver",
 "land_bronze.py": "src/masar/ingest", "profile_bronze.py": "src/masar/ingest",
 "scan_cost.py": "src/masar/econ", "formats_lab.py": "src/masar/econ",
 "enforce_silver.py": "src/masar/delta", "break_it.py": "src/masar/delta", "evolve_demo.py": "src/masar/delta",
 "merge_ops.py": "src/masar/delta", "time_travel.py": "src/masar/delta", "small_files.py": "src/masar/delta",
 "contract.py": "src/masar/quality", "gate.py": "src/masar/quality", "observe.py": "src/masar/quality",
 "pii.py": "src/masar/quality", "build_gold.py": "src/masar/transform", "pipeline.py": "src/masar",
 "training_join.py": "src/masar/serve", "bi_view.py": "src/masar/serve", "leakage_drill.py": "src/masar/serve",
}
for d in range(1, 6):
    sd = f"{FD}/day{d}/scripts"
    for f in os.listdir(sd):
        if f in DEST:
            os.makedirs(DEST[f], exist_ok=True)
            shutil.copy(f"{sd}/{f}", f"{DEST[f]}/{f}")
os.makedirs("/tmp/dry", exist_ok=True)
SKIP_SEC = re.compile(r"start here only|if something goes wrong|end of day|tutorial 6|late-trip|capstone brief|minutes 60", re.I)
SKIP_BLK = re.compile(r"kafka|docker|gps_producer|ingest_gps_stream|ping_windows|git |land_bronze_late|late_trip")
p = subprocess.Popen(["bash"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
def run(cmd):
    p.stdin.write(cmd + "\necho __MARK__$?\n"); p.stdin.flush()
    out = []
    while True:
        l = p.stdout.readline()
        if l.startswith("__MARK__") or not l:
            return l.strip()[8:], out
        out.append(l.rstrip())
run("make clean-lakehouse >/dev/null 2>&1; export PYTHONPATH=$PWD/src")
S = open("/tmp/dry/summary.txt", "w")
n = 0
for d in range(1, 6):
    sec = ""; txt = open(f"{FD}/day{d}/DAY{d}.md").read().split("\n"); i = 0
    while i < len(txt):
        ln = txt[i]
        if ln.startswith("#"):
            sec = ln
        elif ln.startswith("```bash"):
            j = i + 1; blk = []
            while not txt[j].startswith("```"):
                blk.append(txt[j]); j += 1
            i = j; cmd = "\n".join(blk)
            if SKIP_SEC.search(sec) or SKIP_BLK.search(cmd):
                S.write(f"--- D{d} SKIP [{sec}] {blk[0][:70]}\n"); S.flush()
            else:
                n += 1; t = time.time(); rc, out = run(cmd)
                open(f"/tmp/dry/{n:03d}.log", "w").write("\n".join(out))
                keep = [o for o in out if "WARN" not in o][-7:]
                S.write(f"=== #{n} D{d} rc={rc} {int(time.time()-t)}s [{sec}] {blk[0][:90]}\n" + "\n".join("    " + o[:160] for o in keep) + "\n"); S.flush()
        i += 1
S.write("DONE\n"); S.close()
