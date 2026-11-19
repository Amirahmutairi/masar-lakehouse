# Day 1 commands (five-day version): SDA-DSC-214

Every command runs from the repository root in the Codespace terminal. Copy one block at a time. The files you create (`.py`) are provided separately; this page covers only the commands and what you should see.

The text `2>&1 | grep -v WARN` hides harmless Spark warnings. Leave it in.

Day 1 order: setup, L1 formats lab, T1 bronze, T2 scan cost. dbt (T3) moves to Day 2.

## 0. Check your setup

```bash
make doctor
```

```bash
make data
```

```bash
make test
```

Expected: every check passes in `make doctor`, and `make test` prints `4 passed`. `make data` creates 1,850 drivers, 48,200 trips and 120,000 GPS pings.

## Tutorial 1: land and profile bronze

Do T1 before L1: the lab reads the bronze trips table.

### Step 1. Look at the raw data

```bash
head -3 data/raw/trips_2026-06-01.csv
```

```bash
head -c 400 data/raw/gps/sample.ndjson
```

```bash
head -3 data/raw/drivers.csv
```

### Step 2. Create the landing script

Create `src/masar/ingest/land_bronze.py` (file provided).

### Step 3. Run it once

```bash
python -m masar.ingest.land_bronze 2>&1 | grep -v WARN
```

```bash
ls lakehouse/bronze/trips lakehouse/bronze/trips/_delta_log
```

Run the landing script only once. A second run appends everything again and doubles bronze.

### Step 4. Profile what you landed

Create `src/masar/ingest/profile_bronze.py` (file provided), then:

```bash
python -m masar.ingest.profile_bronze 2>&1 | grep -v WARN
```

Expected after one load:
- `rows=48,200  columns=14`
- `dropoff_geohash` null rate about 0.0304; every other column 0.0
- `duplicate trip_id groups: 0`

### Checkpoint: the deliberate second run

```bash
python -m masar.ingest.land_bronze 2>&1 | grep -v WARN
```

```bash
python -m masar.ingest.profile_bronze 2>&1 | grep -v WARN
```

Expected: `rows=96,400` and `duplicate trip_id groups: 48,200`. Bronze is append-only, so duplicates are expected here.

Reset so everyone has one copy:

```bash
rm -rf lakehouse/bronze
```

```bash
python -m masar.ingest.land_bronze 2>&1 | grep -v WARN
```

## L1: the same data in three formats

Create `src/masar/econ/formats_lab.py` (file provided). Bronze must hold one copy of the trips (rows=48,200).

```bash
python -m masar.econ.formats_lab 2>&1 | grep -v WARN
```

Expected (verified in a Codespace; timings vary by machine, sizes should be close):

```
rows written to each copy: {'csv': 48200, 'parquet': 48200, 'delta': 48200}

format   size MB   avg fare by city (s)  transaction log  bad append
csv          9.8                   0.52  no               accepted silently
parquet      1.9                   0.19  no               accepted silently
delta        1.9                   0.47  yes              REJECTED (AnalysisException)
```

What to take from it:
- CSV is about five times larger than Parquet and Delta for the same rows.
- Parquet and Delta have the same size, because Delta stores Parquet files plus a log.
- On this small table Delta is not faster than plain Parquet: the log adds a little work. Do not claim a speed win.
- A text value in a number column is accepted by CSV and Parquet, and rejected by Delta. This is schema enforcement, the reason Day 2 starts with Delta constraints.

Clean up:

```bash
rm -rf lakehouse/scratch/formats
```

## Tutorial 2: scans and the cost model

### Step 1. Compare two query plans

Create `src/masar/econ/scan_cost.py` (file provided), then:

```bash
python -m masar.econ.scan_cost 2>&1 | grep -v WARN
```

Expected: the wide plan reads 14 columns and the narrow plan reads 3 (`trip_id`, `city`, `fare_sar`). The last line is `riyadh trips wide=18,440  narrow=18,440`.

### Step 2. Price the platform

Create `src/masar/econ/cost_model.py` (file provided), then:

```bash
python -m masar.econ.cost_model
```

Expected:

```
{'storage': 276.0, 'compute': 30.62, 'total': 306.62, 'compute_share': 0.1}
```

## End of day

Leave bronze in place for Day 2. Do not run the landing script again.

## If something goes wrong

| Symptom | Fix |
| --- | --- |
| Lock or connection error starting Spark | Close the other Python or Spark session, then retry |
| Rows doubled in bronze | Run `rm -rf lakehouse/bronze`, then land once |
| `formats_lab` says the path is not found | Land bronze first (Tutorial 1) |
| Lab shows `accepted silently` for Delta | `fare_sar` was not cast to a number; use the provided script |
| `ModuleNotFoundError: masar.econ` | Check you are in the repository root with the venv active |
