# Day 4 commands: SDA-DSC-214 (5-day version)

Theme: trust and assembly. Tutorials 7 and 8.

Every command runs from the repository root in the Codespace terminal. Copy one block at a time. The `.py`, `.sql` and `.yml` files are in this day's `scripts/` folder (the trainer shares them); this page covers only the commands and what you should see.

The text `2>&1 | grep -v WARN` hides harmless Spark warnings. Leave it in.

Rules for the whole day:
- Only one Spark session can run at a time. Close any open Python or Spark session before running a script.
- Several steps work on scratch copies under `lakehouse/scratch`. Real silver is never changed by a fault drill.
- Some checks are meant to fail. A traceback that ends in `FAIL-FAST`, `FRESHNESS breach` or `VOLUME anomaly` is the expected result.
- **Stop the Codespace at every break and at the end of the day** (github.com/codespaces, or the Codespaces menu in VS Code). Idle compute is billed against the monthly allowance.

## Start here only if you did not do Days 1 to 3

Create a new Codespace from the `day2-solutions` branch and follow the setup list at the top of `DAY3.md` (data, land bronze once, register, dbt, enforce_silver). Silver then holds 44,371 rows, so your Tutorial 7 numbers say 44,371 where these notes say 44,364. That is expected.

Because you landed bronze today, the freshness check passes. Use `--sla-hours 0` there to force the breach.

Everyone who did Day 3 sees 44,364 until the first `make pipeline` in Tutorial 8.

## Tutorial 7: quality and observability (Hours 1 to 3)

### Hour 1, Step 1. The data contract

Create `src/masar/quality/contract.py` (file provided). Check the real silver table:

```bash
python -m masar.quality.contract silver 2>&1 | grep -v WARN
```

Expected: `rows: 44364`, seven checks, all `passed=True`, and `observed=60.22` on `fare_mean_sane`.

Now a batch with missing keys:

```bash
python -m masar.quality.contract data/fixtures/dirty_batch_keys_null.parquet 2>&1 | grep -v WARN
```

Expected: `trip_id_not_null passed=False bad_rows=40`, and `trip_id_unique` still `passed=True`. The unique check ignores nulls.

### Hour 1, Step 2. The gate

Create `src/masar/quality/gate.py` (file provided). The null-key batch must be blocked:

```bash
python -m masar.quality.gate data/fixtures/dirty_batch_keys_null.parquet 2>&1 | grep -v WARN
```

Expected: a traceback ending `FAIL-FAST: integrity checks failed ['trip_id_not_null']; batch blocked, nothing promoted`.

The bad-fares batch must be quarantined and the rest promoted to a scratch table:

```bash
python -m masar.quality.gate data/fixtures/dirty_batch_bad_fares.parquet 2>&1 | grep -v WARN
```

Expected: `[DATA-ALERT] quarantined 120 rows to .../lakehouse/quarantine/trips`, then `promoted 44,251 new rows` and `table now 44,251`.

Run the same command again. Expected: `promoted 0 new rows`, but another 120 rows are quarantined.

Reset before the next step:

```bash
rm -rf lakehouse/quarantine lakehouse/scratch/silver_gated
```

### Hour 2, Step 3. Freshness, volume and drift

Create `src/masar/quality/observe.py` (file provided). First make two faulty copies of silver. Real silver is not touched:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE, SILVER
from masar.spark import get_spark
s = get_spark("inject")
df = s.read.format("delta").load(SILVER)
df.limit(df.count() // 2).write.format("delta").mode("overwrite").save(str(LAKEHOUSE / "scratch" / "silver_half"))
df.withColumn("fare_sar", F.col("fare_sar") * 1.3).write.format("delta").mode("overwrite").save(str(LAKEHOUSE / "scratch" / "silver_drift"))
print("scratch copies ready")
EOF
```

Expected: `scratch copies ready`.

```bash
python -m masar.quality.observe --check freshness 2>&1 | grep -v WARN
```

Expected: a traceback ending `FRESHNESS breach: ... last updated <time>`. If it prints `freshness ok` instead, silver was landed less than 6 hours ago. Run it again with `--sla-hours 0`.

```bash
python -m masar.quality.observe --check freshness --sla-hours 1000 2>&1 | grep -v WARN
```

Expected: `freshness ok`.

```bash
python -m masar.quality.observe --check volume --path lakehouse/scratch/silver_half 2>&1 | grep -v WARN
```

Expected: `VOLUME anomaly: 22,182 vs expected ~44,371`.

```bash
python -m masar.quality.observe --check drift --path lakehouse/scratch/silver_drift 2>&1 | grep -v WARN
```

Expected: `[DRIFT] fare mean 78.3 vs baseline 60.2`.

```bash
rm -rf lakehouse/scratch/silver_half lakehouse/scratch/silver_drift
```

### Hour 3, Step 4. PII and retention

Create `src/masar/quality/pii.py` (file provided). Retention is destructive, so work on a scratch copy of the GPS pings. This also shows the time range:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark
s = get_spark("prep")
df = s.read.format("delta").load(str(LAKEHOUSE / "bronze" / "gps_pings"))
df.write.format("delta").mode("overwrite").save(str(LAKEHOUSE / "scratch" / "gps_retention"))
df.agg(F.min("event_ts").alias("min"), F.max("event_ts").alias("max")).show(truncate=False)
EOF
```

Expected: 2026-06-01 to 2026-06-02.

Dry run with the real clock:

```bash
python -m masar.quality.pii --path lakehouse/scratch/gps_retention 2>&1 | grep -v WARN
```

Expected: `0 of 120,000 rows are past 180 days`. The data is only about 161 days old.

Dry run with a simulated clock:

```bash
python -m masar.quality.pii --path lakehouse/scratch/gps_retention --as-of "2026-11-28 12:00" 2>&1 | grep -v WARN
```

Expected: `43,319 of 120,000 rows are past 180 days`.

Delete:

```bash
python -m masar.quality.pii --path lakehouse/scratch/gps_retention --as-of "2026-11-28 12:00" --apply 2>&1 | grep -v WARN
```

Expected: `table now 76,681 rows`.

Check whether the deleted rows are really gone. This prints two numbers per version: the log count and a real read of the files:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark
s = get_spark("tt")
p = str(LAKEHOUSE / "scratch" / "gps_retention")
for v in (0, 1):
    d = s.read.format("delta").option("versionAsOf", v)
    log_count = d.load(p).count()
    try:
        real = d.load(p).select(F.sum(F.length("trip_id"))).collect()[0][0]
        print(f"version {v}: count={log_count:,}  real read OK (checksum {real})")
    except Exception as e:
        print(f"version {v}: count={log_count:,}  real read FAILED ({type(e).__name__})")
EOF
```

Expected now: version 0 still reads 120,000 rows, because the old files are still on disk. Now remove them physically:

```bash
python -m masar.quality.pii --path lakehouse/scratch/gps_retention --as-of "2026-11-28 12:00" --apply --purge 2>&1 | grep -v WARN
```

Expected: `vacuumed: old files physically removed`. Run the check script above again. Expected: version 0 `real read FAILED`, version 1 `real read OK`. The count for version 0 may still say 120,000: Delta answers `count()` from its log. Never use `count()` to prove a deletion.

```bash
rm -rf lakehouse/scratch/gps_retention
```

## Tutorial 8: gold and one command (Hours 4 and 5)

Create `src/masar/transform/build_gold.py` and `src/masar/pipeline.py` (files provided). Run the whole pipeline:

```bash
make pipeline 2>&1 | grep -v WARN
```

Expected: a line `[...] OK` for each task, seven checks all `passed=True`, `zone_hourly_demand rows: 120`, `trip_features rows: 44,371`, and `[optimize_vacuum] OK`. The first `make pipeline` brings silver back to 44,371 rows (see the finding below).

Verify the gold tables:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from pyspark.sql import functions as F
from masar.config import GOLD, SILVER
from masar.spark import get_spark
s = get_spark("verify")
silver = s.read.format("delta").load(SILVER).count()
d = s.read.format("delta").load(f"{GOLD}/zone_hourly_demand")
f = s.read.format("delta").load(f"{GOLD}/trip_features")
print("silver rows        :", silver)
print("demand rows        :", d.count())
print("demand sum(trips)  :", d.agg(F.sum("trips")).first()[0])
print("demand grain dups  :", d.groupBy("city", "date", "hour").count().filter("count > 1").count())
print("features rows      :", f.count())
print("features dup trips :", f.groupBy("trip_id").count().filter("count > 1").count())
EOF
```

Expected: silver 44,371, demand rows 120, demand sum 44,371, both duplicate counts 0, features rows 44,371.

Run the pipeline a second time and verify again. Every number must be identical:

```bash
make pipeline 2>&1 | grep -v WARN | tail -12
```

### Hour 5: failure drill

Make one task fail on purpose:

```bash
MASAR_FAIL_TASK=build_silver make pipeline 2>&1 | grep -v WARN
```

Expected: three `attempt ... failed: injected failure` lines, then `PIPELINE HALTED`. No gold task runs. Now recover with the same command as before:

```bash
make pipeline 2>&1 | grep -v WARN | tail -12
```

Expected: green, with the same counts and no duplicates.

### The finding: erased trips came back

Look at the history of silver:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from delta.tables import DeltaTable
from masar.config import SILVER
from masar.spark import get_spark
s = get_spark("hist")
(DeltaTable.forPath(s, SILVER).history()
    .select("version", "operation", "operationMetrics.numTargetRowsInserted",
            "operationMetrics.numTargetRowsUpdated", "operationMetrics.numTargetRowsDeleted")
    .orderBy("version").show(30, truncate=False))
EOF
```

Expected (if you ran the Day 2 erase): a `DELETE`, then a `MERGE` with 7 rows inserted, then later merges that update all 44,371 rows with nothing new. The erased rider came back because bronze still holds the rows. Discuss how to prevent it.

## Hour 5, last 15 minutes: capstone brief

No commands. Open `capstone/CAPSTONE.md` on the `capstone` branch with the trainees and walk through the eight steps.

## End of day

Remove scratch data, then stop the Codespace:

```bash
rm -rf lakehouse/scratch lakehouse/quarantine
```

## If something goes wrong

- `PATH_NOT_FOUND` on `silver_half` or `silver_drift`: run the faulty-copies block in Tutorial 7, Step 3 first.
- `ValueError` from `--as-of`: use the form `"2026-11-28 12:00"`, date and time together.
- `VACUUM` fails with `TABLE_OR_VIEW_NOT_FOUND`: the SQL form needs an absolute path. Use the provided `pii.py`, which calls the Python API.
- `replaceWhere` error saying data does not match: use the provided `build_gold.py`, which derives the window from the data.
- Schema mismatch when overwriting a gold table: add `.option("overwriteSchema", "true")` to that write.
- Metastore lock or a hanging dbt step: another Spark session is open. Close it and retry.
- Counts doubled after a pipeline rerun: bronze was landed twice. Run it once, or `make clean-lakehouse` and start again.
