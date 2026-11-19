# Day 2 commands: SDA-DSC-214 (5-day version)

Theme: ELT and the enforced table. Tutorials 3 and 4.

Every command runs from the repository root in the Codespace terminal. Copy one block at a time. The `.py`, `.sql` and `.yml` files are in this day's `scripts/` folder (the trainer shares them); this page covers only the commands and what you should see.

The text `2>&1 | grep -v WARN` hides harmless Spark warnings. Leave it in.

Rules for the whole day:
- Only one Spark session can run at a time. Close any open Python or Spark session before running a script.
- Do not run `dbt run` outside the steps below.
- **Stop the Codespace at every break and at the end of the day** (github.com/codespaces, or the Codespaces menu in VS Code). Idle compute is billed against the monthly allowance.

## Hour 1: recap

No commands. Recap quiz on the slides, then the ETL vs ELT and idempotency slides.

## Start here only if you did not do Day 1

```bash
make data
```

```bash
python -m masar.ingest.land_bronze 2>&1 | grep -v WARN
```

Run the landing script only once. Bronze must then hold 48,200 trips and 1,850 drivers.

## Tutorial 3: dbt staging and incremental silver (Hours 2 and 3)

Close any open Python or Spark session first. The local metastore allows one Spark session at a time.

### Step 0. Prerequisites

Bronze must hold trips and drivers from Tutorial 1.

```bash
make register
```

```bash
make dbt-deps
```

```bash
make dbt-debug
```

Expected: all checks passed. `dbt deps` prints a deprecated-version notice and an update notice; ignore both. Do not upgrade.

### Step 1. Declare the source

Create `dbt/masar/models/staging/masar/_masar__sources.yml` (file provided).

### Step 2. Create the staging models

Create `stg_trips.sql` and `stg_drivers.sql` in `dbt/masar/models/staging/masar/` (files provided).

### Step 3. Run staging

```bash
cd dbt/masar
```

```bash
dbt source freshness
```

```bash
dbt run --select stg_trips stg_drivers
```

```bash
cd ../..
```

### Hour 3 checkpoint

Run from the repository root with no dbt command running:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from masar.spark import get_spark
s = get_spark("check")
print(s.sql("select count(*) n, count(distinct trip_id) u from default.stg_trips").collect())
print(s.sql("select count(*) from default.stg_drivers").collect())
EOF
```

Expected: `[Row(n=48200, u=48200)]` and `[Row(count(1)=1850)]`.

### Step 4. The silver model

Create `dbt/masar/models/marts/silver/silver_trips.sql` (file provided).

### Step 5. Declare the tests

Create `dbt/masar/models/marts/silver/_silver__models.yml` (file provided).

### Step 6. Build and test

```bash
cd dbt/masar
```

```bash
dbt run --select silver_trips
```

```bash
dbt test --select silver_trips
```

```bash
cd ../..
```

Expected: the run succeeds, and the tests pass (5 declared tests in the project). Silver holds 44,371 completed trips.

### Step 7. The late-trip drill (trainer demonstration)

**Do not run this block in your own Codespace.** It adds one trip to bronze, which makes every later checkpoint in the week one row higher (silver 44,372 instead of 44,371), including the capstone. Watch the trainer run it, and compare with the expected output below.

The trainer lands one late trip into bronze:

```bash
python -c "from masar.ingest.land_bronze import land_trips; land_trips('data/fixtures/late_trip.csv')" 2>&1 | grep -v WARN
```

Re-run silver:

```bash
cd dbt/masar && dbt run --select silver_trips && cd ../..
```

Read the Delta history:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from masar.spark import get_spark
s = get_spark("history")
h = DeltaTable.forPath(s, "lakehouse/silver/trips").history()
(h.select("version", "operation",
          F.col("operationMetrics")["numTargetRowsInserted"].alias("inserted"),
          F.col("operationMetrics")["numTargetRowsUpdated"].alias("updated"))
  .orderBy("version").show(truncate=False))
print(s.sql("select count(*) from silver.trips").collect())
EOF
```

Expected: version 0 is the create, version 1 is a MERGE with inserted 1 and updated 44,371, and the count is 44,372.

Run the dbt step once more and look at the history again to see the repeat-run proof (version 2: inserted 0, updated 44,372, count stays 44,372):

```bash
cd dbt/masar && dbt run --select silver_trips && cd ../..
```

## Tutorial 4: an enforced silver table (Hours 4 and 5)

### Hour 4, Step 1. Add the constraints

Create `src/masar/delta/enforce_silver.py` (file provided), then run it once:

```bash
python -m masar.delta.enforce_silver 2>&1 | grep -v WARN
```

Expected: the property list includes `delta.constraints.positive_fare` and `delta.constraints.valid_duration`. A second run fails with "constraint already exists", which is harmless. If a bad row blocks it (`DELTA_NEW_CHECK_CONSTRAINT_VIOLATION`), delete rows with `fare_sar <= 0` and run it again.

### Step 2. Break it on purpose

Run this after Step 1, or the bad batch is appended.

```bash
python -m masar.delta.break_it 2>&1 | grep -v WARN
```

Expected: two `REJECTED` lines, and the three counts are identical. The string-into-double write fails with `DELTA_CONSTRAINT_DATA_TYPE_MISMATCH`.

### Step 3. Read the transaction log

```bash
ls lakehouse/silver/trips/_delta_log
```

```bash
grep -o '"delta.constraints[^}]*' lakehouse/silver/trips/_delta_log/*.json | head
```

### Hour 5, Step 4. Evolve a schema on purpose

```bash
python -m masar.delta.evolve_demo 2>&1 | grep -v WARN
```

Expected: one `REJECTED` line, then 10 rows, 5 with `RAMADAN10` and 5 null.

## End of day

Commit your scripts, but never add `lakehouse/` or `data/`. Stop the Codespace.

## If something goes wrong

| Symptom | Fix |
| --- | --- |
| Lock or connection error starting Spark or dbt | Close the other Python or Spark session, then retry |
| dbt cannot find the source | Run `make register` after landing bronze |
| Rows doubled in bronze | Run `rm -rf lakehouse/bronze`, land once, then `make register` |
| `ParseException` on a history query | Use `DeltaTable.forPath(spark, path).history()` |
| `DELTA_NEW_CHECK_CONSTRAINT_VIOLATION` when adding a constraint | Delete rows with `fare_sar <= 0` and run Step 1 again |
| `DELTA_FAILED_TO_MERGE_FIELDS` on `duration_min` | Keep the cast loop in `break_it.py`; the silver model must cast `duration_min` to double |
| Bad batch got into silver | Step 2 ran before Step 1; delete the rows and run Step 1 |
| Freshness warning the next day | Normal: bronze is older than 6 hours |
