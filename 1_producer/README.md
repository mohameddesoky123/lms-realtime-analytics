# 1_producer - LMS event simulator + Kafka producer (Member 1)

Simulates thousands of learners using an online learning platform and sends their events to the
Kafka topic `lms_raw` in real time. Part of the *Real-time LMS Analytics* project (Kafka -> NiFi -> Kafka -> Spark -> DB).

The output follows the shared contract (project plan, Section 4). Roughly 3-5% of events are
**intentionally dirty** so that NiFi has real ETL work to do.

## Files

| File | What it does |
|------|--------------|
| `producer.py` | Command line tool. Sends events to Kafka, prints them, or dumps them to a file. |
| `simulator.py` | The simulator engine (virtual learners, state machine, dirty data, late events). |
| `config.yaml` | Courses, devices, regions, rates, planted patterns, dirty-data rates. |
| `validate_contract.py` | Checks a JSONL file against the contract. Useful for every team member. |
| `create_topics.sh` | Creates `lms_raw`, `lms_clean`, `lms_dead_letter`. |
| `tests/test_simulator.py` | 11 automated tests (contract, error rates, planted patterns, determinism). |
| `../contracts/sample_raw.jsonl` | 1,500 raw events (with errors) - for Member 2 (NiFi). |
| `../contracts/sample_clean.jsonl` | 3,000 clean events - for Members 3, 4, 5 (Spark). |

## Quick start

```bash
cd 1_producer
python -m venv .venv && source .venv/bin/activate      # Windows (WSL2): same commands
pip install -r requirements.txt

pytest -q                                              # 11 tests should pass

# 1) see events on the screen (no Kafka needed)
python producer.py --stdout --rate 5 --duration 20

# 2) with Kafka running (docker compose from Member 6):
./create_topics.sh
python producer.py --rate 100 --users 500 --duration 600

# 3) watch the topic
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic lms_raw --from-beginning --max-messages 5
```

## Command line options

| Option | Default | Meaning |
|--------|---------|---------|
| `--rate` | 50 | Target events per second (the simulator adapts the number of active sessions) |
| `--users` | 500 | Size of the virtual user pool (= max concurrent learners) |
| `--duration` | 600 | Seconds to run. `0` = until Ctrl+C. In `--dump` mode this is *virtual* time |
| `--max-events` | 0 | Stop after N events (0 = no limit) |
| `--bootstrap` | localhost:9092 | Kafka address *from the host*. (Inside Docker it would be `kafka:29092`) |
| `--topic` | lms_raw (lms_clean with `--clean`) | Target topic |
| `--dump FILE` | - | Write JSONL to a file, no waiting (fast) |
| `--stdout` | - | Print events in real time instead of sending to Kafka |
| `--clean` | - | Clean shape: no dirty data, `processed_at` added (what NiFi outputs) |
| `--dirty-rate` | 1.0 | Multiplier of the error rates in `config.yaml` (0 = none, 2 = double) |
| `--late-rate` | 0.02 | Probability of a late event (30-90 s old) |
| `--seed` | random | Same seed = same data |
| `--start` | now | Virtual start time `2026-11-14T10:00:00Z` (with `--dump`) |

## How the data looks

* Event types: `session_start, video_play, video_pause, video_seek, buffering, quiz_submit, lesson_navigate, session_end`.
* Key of every Kafka message = `user_id` (same user -> same partition -> order is kept).
* Times are always UTC. `event_time` is the time the event happened (late events are older than their arrival time).

**Planted patterns** (so the dashboard shows something interesting):

1. Module `CS101-M03` has a very high drop-off around second 210-270.
2. Group `mobile` + `upper_egypt` has much longer buffering (average above 3000 ms; everyone else about 1000 ms).
3. The number of active learners rises and falls in waves (period 10 minutes).

**Intentional errors** (rates in `config.yaml`, section `dirty`): null `user_id`/`course_id`, duplicate `event_id`,
upper-case or padded `event_type`, `event_time` as epoch-ms number, `device`/`region` with other case or spaces,
and out-of-range values (negative position, buffer > 600000, score > 100).

## Regenerating the sample files

```bash
python producer.py --dump ../contracts/sample_raw.jsonl   --max-events 1500 --users 200 --rate 30 --seed 1 --start 2026-11-14T10:00:00Z
python producer.py --dump ../contracts/sample_clean.jsonl --max-events 3000 --users 200 --rate 30 --seed 2 --start 2026-11-14T10:00:00Z --clean

python validate_contract.py ../contracts/sample_clean.jsonl        # must say: invalid lines: 0
python validate_contract.py ../contracts/sample_raw.jsonl --raw    # shows which errors are inside
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Connect ... failed: Connection refused` | Kafka is not running, or wrong `--bootstrap`. From the host use `localhost:9092`. |
| Messages sent but not seen | Check the topic name, and read with `--from-beginning`. |
| `ModuleNotFoundError: yaml / confluent_kafka` | `pip install -r requirements.txt` inside the virtual environment. |
| `Permission denied: create_topics.sh` | `chmod +x create_topics.sh` |
| Container is not called `kafka` | `KAFKA_CONTAINER=<name> ./create_topics.sh` |
