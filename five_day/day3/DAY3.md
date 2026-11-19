# Day 3 commands: SDA-DSC-214 (5-day version)

Theme: Delta in depth and streaming. Tutorials 5 and 6.

Every command runs from the repository root in the Codespace terminal. Copy one block at a time. The `.py`, `.sql` and `.yml` files are in this day's `scripts/` folder (the trainer shares them); this page covers only the commands and what you should see.

The text `2>&1 | grep -v WARN` hides harmless Spark warnings. Leave it in.

Rules for the whole day:
- Only one Spark session can run at a time. Close any open Python or Spark session before running a script.
- Do not run `dbt run` during Tutorial 5. It would re-merge the erased rider from bronze.
- Tutorial 6 needs Docker (Kafka). Start it only in Hour 4 and stop it at the end of the day.
- **Stop the Codespace at every break and at the end of the day** (github.com/codespaces, or the Codespaces menu in VS Code). Idle compute is billed against the monthly allowance.

## Hour 1: recap

Recap quiz on the slides, then Tutorial 5.

## Start here only if you did not do Days 1 and 2

Create a new Codespace from the `day2-solutions` branch, then run once, in order:

```bash
make data
```

```bash
python -m masar.ingest.land_bronze 2>&1 | grep -v WARN
```

```bash
make register
```

```bash
make dbt-deps
```

```bash
cd dbt/masar && dbt run && dbt test && cd ../..
```

```bash
python -m masar.delta.enforce_silver 2>&1 | grep -v WARN
```

Run the landing script only once. Silver should hold 44,371 rows.

## Tutorial 5: upsert, audit, compact (Hours 1 to 3)

### Hour 1, Step 1. The corrections MERGE

Run it twice:

```bash
python -m masar.delta.merge_ops corrections 2>&1 | grep -v WARN
```

```bash
python -m masar.delta.merge_ops corrections 2>&1 | grep -v WARN
```

Expected: the first run shows the five fares change and `MERGE committed as v3: rows_updated=5`. The second run prints `no changes: nothing committed`.

### Hour 1, Step 2. Erase a rider

```bash
python -m masar.delta.merge_ops erase 2>&1 | grep -v WARN
```

Expected: `erased rider_00042: v4 deleted=7  rows 44371 -> 44364`.

### Hour 2, Step 3. Time travel

```bash
python -m masar.delta.time_travel history 2>&1 | grep -v WARN
```

```bash
python -m masar.delta.time_travel rider 2>&1 | grep -v WARN
```

Expected: the history shows v0 create, v1 and v2 ADD CONSTRAINT, v3 MERGE, v4 DELETE. The rider has 7 rows at v0 to v3 and 0 at v4.

### Hour 3, Steps 4 and 5. Small files, OPTIMIZE, VACUUM

```bash
python -m masar.delta.small_files make 2>&1 | grep -v WARN
```

This takes a minute or two. Expected: `numFiles=100`.

```bash
python -m masar.delta.small_files maintain 2>&1 | grep -v WARN
```

Expected: 100 files become 1 (`numFilesAdded=1`, `numFilesRemoved=100`), and the VACUUM dry run lists nothing.

## Tutorial 6: Kafka to Delta (Hours 4 and 5)

### Hour 4, Step 1. Start Kafka and create the topic

```bash
docker --version && docker compose version
```

```bash
make kafka-up
```

```bash
make kafka-topic
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic masar.gps.pings --bootstrap-server localhost:9092
```

Expected: `PartitionCount: 3` and three partition lines. The warning about periods and underscores in the topic name is harmless.

### Step 2. Produce GPS pings

```bash
python -m masar.stream.gps_producer 2000
```

Expected: `sent 2000 pings; per partition: {0: 560, 1: 720, 2: 720}`.

### Step 3. Check the data in Kafka

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic masar.gps.pings --from-beginning --max-messages 3 --bootstrap-server localhost:9092
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --topic masar.gps.pings --bootstrap-server localhost:9092
```

Expected: three pings from one trip, and offsets 560, 720 and 720 (total 2000).

### Hour 5, Steps 4 and 5. Stream Kafka into bronze

Create `src/masar/stream/schemas.py` and `src/masar/stream/ingest_gps_stream.py` (files provided), then start the stream:

```bash
python -m masar.stream.ingest_gps_stream 2>&1 | grep -v WARN
```

The job never finishes on its own and prints nothing. That is normal.

### Step 6. Watch the commits arrive

In a second terminal:

```bash
ls lakehouse/bronze/gps_stream/_delta_log | wc -l
```

Expected: 5 after the first 2000 pings. Do not start any other Spark process while the stream is running.

### Checkpoint: kill and restart

In the second terminal, send 2000 more pings:

```bash
python -m masar.stream.gps_producer 2000
```

While commits are still arriving, press Ctrl+C in the ingest terminal, then restart it:

```bash
python -m masar.stream.ingest_gps_stream 2>&1 | grep -v WARN
```

Let it settle, stop it with Ctrl+C, and check the result:

```bash
python - <<'EOF' 2>&1 | grep -v WARN
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark
s = get_spark("check")
path = str(LAKEHOUSE / "bronze" / "gps_stream")
(DeltaTable.forPath(s, path).history()
    .select("version", "operation", F.col("operationMetrics")["numOutputRows"].alias("rows"))
    .orderBy("version").show(truncate=False))
df = s.read.format("delta").load(path)
print("rows:", df.count(), " distinct (trip_id, event_ts):", df.select("trip_id", "event_ts").distinct().count())
EOF
```

Expected: `rows: 4000`, which equals the Kafka total. `distinct` is 2000 because the producer replays the same 2000 lines and ran twice. Bronze keeps what Kafka holds; deduplication happens in silver.

### Step 7. Windowed counts

Stop the ingest job first, then:

```bash
python -m masar.stream.ping_windows 2>&1 | grep -v WARN
```

Expected: 5-minute windows per city, every count even (for example Mecca 60, Riyadh 120), because the pings are duplicated.

## End of day

```bash
make kafka-down
```

Commit your scripts, but never add `lakehouse/` or `data/`. Stop the Codespace.

## If something goes wrong

| Symptom | Fix |
| --- | --- |
| Erased rider's trips came back | `dbt run` re-merged them from bronze; do not run dbt in Tutorial 5 |
| Derby lock error | Another Spark session is open; close it, one at a time |
| `make kafka-up` fails or hangs | Run `docker ps`, wait for the image pull, check the network |
| Ingest job prints nothing and never ends | Normal for a stream; watch the `_delta_log` count in a second terminal |
| `Failed to find data source: kafka` | The first Spark call in the process must be `get_spark(kafka=True)` |
