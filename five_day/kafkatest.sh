#!/bin/bash
# Trainer tool: dry run of Tutorial 6 (Kafka to Delta). Run from the repo root with the venv active.
L=/tmp/kt.log; : > $L
say(){ echo "$*" | tee -a $L; }
run(){ say "--- $*"; "$@" 2>&1 | grep -v WARN | tail -${N:-6} | tee -a $L; }
export PYTHONPATH=$PWD/src
T="docker compose exec -T kafka /opt/kafka/bin"
rm -rf lakehouse/bronze/gps_stream lakehouse/_checkpoints
run make kafka-up
for i in 1 2 3 4 5 6; do sleep 10; make kafka-topic >>$L 2>&1 && break; done
N=5 run $T/kafka-topics.sh --describe --topic masar.gps.pings --bootstrap-server localhost:9092
N=2 run python -m masar.stream.gps_producer 2000
N=3 run $T/kafka-console-consumer.sh --topic masar.gps.pings --from-beginning --max-messages 3 --bootstrap-server localhost:9092
N=4 run $T/kafka-get-offsets.sh --topic masar.gps.pings --bootstrap-server localhost:9092
(nohup python -m masar.stream.ingest_gps_stream > /tmp/ingest.out 2>&1 &)
for i in $(seq 1 40); do sleep 10; n=$(ls lakehouse/bronze/gps_stream/_delta_log 2>/dev/null | wc -l); [ "$n" -ge 5 ] && break; done
say "delta_log entries after first 2000 pings: $n (expected 5)"
python -m masar.stream.gps_producer 2000 2>&1 | tail -1 | tee -a $L
sleep 12; pkill -INT -f ingest_gps_stream; sleep 8; pkill -9 -f ingest_gps_stream; sleep 3
say "killed mid-flight; restarting"
(nohup python -m masar.stream.ingest_gps_stream > /tmp/ingest2.out 2>&1 &)
sleep 100; pkill -INT -f ingest_gps_stream; sleep 10; pkill -9 -f ingest_gps_stream; sleep 3
python - <<'EOF' 2>&1 | grep -v WARN | tee -a $L
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark
s = get_spark("check")
df = s.read.format("delta").load(str(LAKEHOUSE / "bronze" / "gps_stream"))
print("rows:", df.count(), " distinct (trip_id, event_ts):", df.select("trip_id", "event_ts").distinct().count())
EOF
N=8 run python -m masar.stream.ping_windows
make kafka-down >>$L 2>&1; say "kafka down"; say FINISHED
