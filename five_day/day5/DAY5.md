# Day 5 commands: SDA-DSC-214 (5-day version)

Theme: serve and capstone. Tutorial 9 and the capstone. Minutes 0 to 260 are teaching time; the final 40 minutes are the survey.

Every command runs from the repository root in the Codespace terminal. Copy one block at a time. The `.py`, `.sql` and `.yml` files are in this day's `scripts/` folder (the trainer shares them); this page covers only the commands and what you should see.

The text `2>&1 | grep -v WARN` hides harmless Spark warnings. Leave it in.

Rules for the whole day:
- Only one Spark session can run at a time. Close any open Python or Spark session before running a script.
- Tutorial 9 needs the gold tables from Day 4. If they are missing, run `make pipeline` once.
- **Stop the Codespace at every break and at the end of the day** (github.com/codespaces, or the Codespaces menu in VS Code). Idle compute is billed against the monthly allowance.

## Minutes 0 to 60: recap and Tutorial 9

Recap quiz on the slides, then:

## Tutorial 9: serve AI and BI (Hour 1)

Create the package marker, then the three files in `src/masar/serve/` (provided):

```bash
touch src/masar/serve/__init__.py
```

### Step 1. Training set

```bash
python -m masar.serve.training_join 2>&1 | grep -v WARN | tail -12
```

Expected: `training rows: 44,371`, and the columns do not include `fare_per_km`.

### Step 2. A safe table for BI

```bash
python -m masar.serve.bi_view 2>&1 | grep -v WARN
```

Expected: `bi_trips_safe rows: 44,371`, with `rider_pseudonym`, `driver_pseudonym` and `dropoff_area`.

Check that no raw identifier crossed the boundary:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from pyspark.sql import functions as F
from masar.config import GOLD, SILVER
from masar.spark import get_spark
s = get_spark("bi-check")
bi = s.read.format("delta").load(f"{GOLD}/bi_trips_safe")
silver = s.read.format("delta").load(SILVER)
print("columns            :", bi.columns)
print("raw ids present    :", [c for c in ("rider_id", "driver_id", "dropoff_geohash", "lat", "lon") if c in bi.columns])
print("distinct riders    :", silver.select("rider_id").distinct().count())
print("distinct pseudonyms:", bi.select("rider_pseudonym").distinct().count())
bi.groupBy("city").count().orderBy("city").show()
rid = silver.select("rider_id").first()[0]
unsalted = s.range(1).select(F.sha2(F.lit(rid), 256).alias("h")).first()[0]
print("unsalted guess matches:", bi.filter(F.col("rider_pseudonym") == unsalted).count())
EOF
```

Expected: `raw ids present: []`, 17889 riders and 17889 pseudonyms, city counts adding up to 44,371, and `unsalted guess matches: 0`.

### Step 3. The leakage drill

```bash
python -m masar.serve.leakage_drill 2>&1 | grep -v WARN | tail -5
```

Expected: R2 about 0.000 for set A (prior-hour features), about 0.41 for set B (adds `fare_per_km`), and about 0.000 for set C. The value for B can differ by 0.01 between runs. The jump in B is the leak: the fare is only known after the trip ends.

### Extension (about 15 minutes, discussion only)

No new code. Using the outputs you already have, answer two questions in pairs: what can the model see at prediction time, and which table may BI read? Then predict whether one new feature would leak before you run anything.

## Minutes 60 to 260: capstone

Everything for the capstone is on the `capstone` branch. Stay in the same Codespace. First save your Day 1 to 5 work, then switch and start from an empty lakehouse:

```bash
git add -A && git commit -m "Day 1-5 work"
```

```bash
git push -u origin HEAD
```

```bash
git fetch origin capstone && git switch capstone
```

```bash
make clean-lakehouse
```

`make clean-lakehouse` deletes the lakehouse you built this week, so capstone step 1 lands bronze exactly once and the counts match the checklist. `data/` is kept. Do not run it before you have pushed.

If your Codespace is broken or you joined late: open a new Codespace on the `capstone` branch, then run `make setup`, `source .venv/bin/activate`, `make doctor` and `make data`. Skip the commands above.

Follow `capstone/CAPSTONE.md`. Check your counts against `capstone/checks/expected_counts.md`. Timing: 0 to 10 brief, 10 to 100 steps 1 to 4, 100 to 110 break (stop the Codespace if you leave your desk), 110 to 160 steps 5 to 7, 160 to 185 README and submit, 185 to 200 pair review.

## End of day

Push your branch, then stop the Codespace (github.com/codespaces). Do not commit `lakehouse/` or `data/`.

## If something goes wrong

- `ModuleNotFoundError: masar.serve`: run `touch src/masar/serve/__init__.py`.
- Schema mismatch when overwriting the training set: add `.option("overwriteSchema", "true")` to that write.
- Metastore lock: another Spark session is open. Close it and retry.
- Counts differ from the checkpoints: bronze was landed twice. `make clean-lakehouse`, then `make data`, land once, and `make pipeline`.
