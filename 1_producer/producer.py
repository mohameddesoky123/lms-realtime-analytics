"""
LMS event producer (Member 1).

Generates simulated learning-platform events and sends them to Kafka (topic lms_raw),
or writes them to a file / the screen.

Examples
--------
  # send to Kafka for 10 minutes at about 100 events/second
  python producer.py --rate 100 --users 500 --duration 600

  # see what would be sent, without Kafka (real time, printed to the screen)
  python producer.py --stdout --rate 5 --duration 20

  # generate the sample files for the team (fast, no waiting)
  python producer.py --dump ../contracts/sample_raw.jsonl   --max-events 400  --users 80 --rate 20 --seed 1 --start 2026-11-14T10:00:00Z
  python producer.py --dump ../contracts/sample_clean.jsonl --max-events 3000 --users 200 --rate 30 --seed 2 --start 2026-11-14T10:00:00Z --clean
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone

from simulator import Simulator, load_config


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="LMS event simulator / Kafka producer",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--config", default=None, help="path to config.yaml (default: next to this file)")
    ap.add_argument("--rate", type=float, default=50, help="target events per second (default 50)")
    ap.add_argument("--users", type=int, default=500, help="size of the virtual user pool (default 500)")
    ap.add_argument("--duration", type=float, default=600,
                    help="seconds to run; 0 = until Ctrl+C (default 600). In --dump mode it is virtual time")
    ap.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = no limit)")
    ap.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers (default localhost:9092)")
    ap.add_argument("--topic", default=None, help="Kafka topic (default lms_raw, or lms_clean with --clean)")
    ap.add_argument("--dump", metavar="FILE", help="write events to a JSONL file instead of Kafka (no waiting)")
    ap.add_argument("--stdout", action="store_true", help="print events in real time instead of sending to Kafka")
    ap.add_argument("--clean", action="store_true",
                    help="clean shape: no dirty data and with processed_at (what NiFi would output)")
    ap.add_argument("--dirty-rate", type=float, default=1.0,
                    help="multiplier of the error rates in config.yaml (1 = table 4.3, 0 = no errors)")
    ap.add_argument("--late-rate", type=float, default=None, help="probability of a late event (default from config)")
    ap.add_argument("--seed", type=int, default=None, help="random seed (same seed = same data)")
    ap.add_argument("--start", default=None, help="virtual start time, ISO UTC (only useful with --dump)")
    ap.add_argument("--report-every", type=float, default=5, help="print statistics every N seconds")
    return ap.parse_args(argv)


def build_simulator(a):
    cfg = load_config(a.config)
    start = None
    if a.start:
        start = datetime.strptime(a.start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    return Simulator(cfg, users=a.users, rate=a.rate, seed=a.seed, start=start,
                     clean=a.clean, dirty_scale=a.dirty_rate, late_rate=a.late_rate)


def finished(a, sim, due, count):
    if a.max_events and count >= a.max_events:
        return True
    if a.duration and (due - sim.t0) >= a.duration:
        return True
    return False


def run_dump(a, sim):
    n = 0
    with open(a.dump, "w", encoding="utf-8") as f:
        for due, ev in sim.stream():
            if finished(a, sim, due, n):
                break
            f.write(json.dumps(ev, separators=(",", ":")) + "\n")
            n += 1
    print("wrote %d events to %s" % (n, a.dump))


def run_realtime(a, sim, send, flush=None):
    """Common real-time loop: wait until each event is due, then call send(ev)."""
    n = 0
    last_report = time.time()
    last_n = 0
    try:
        for due, ev in sim.stream():
            if finished(a, sim, due, n):
                break
            wait = due - time.time()
            if wait > 0:
                time.sleep(wait)
            send(ev)
            n += 1
            now = time.time()
            if now - last_report >= a.report_every:
                print("[%s] sent %d events (%.1f/s), active sessions: %d" % (
                    datetime.now().strftime("%H:%M:%S"), n, (n - last_n) / (now - last_report),
                    len(sim.active)), file=sys.stderr)
                last_report, last_n = now, n
    except KeyboardInterrupt:
        print("\nstopped by user", file=sys.stderr)
    finally:
        if flush:
            flush()
        print("done: %d events" % n, file=sys.stderr)


def run_kafka(a, sim):
    from confluent_kafka import Producer          # imported here so --dump / --stdout work without Kafka

    topic = a.topic or ("lms_clean" if a.clean else "lms_raw")
    errors = {"n": 0}

    def on_delivery(err, msg):                    # called by poll() for every message
        if err is not None:
            errors["n"] += 1
            if errors["n"] <= 5:
                print("DELIVERY FAILED: %s" % err, file=sys.stderr)

    p = Producer({"bootstrap.servers": a.bootstrap, "client.id": "lms-simulator",
                  "acks": "all", "linger.ms": 20, "message.timeout.ms": 30000})
    print("sending to %s (topic %s)" % (a.bootstrap, topic), file=sys.stderr)

    def send(ev):
        key = ev.get("user_id")
        key = str(key).encode("utf-8") if key else None      # key = user_id (same user -> same partition)
        value = json.dumps(ev, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                p.produce(topic, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:                   # local queue full: let it drain
                p.poll(0.5)
        p.poll(0)

    def flush():
        remaining = p.flush(15)
        print("flush done, undelivered: %d, delivery errors: %d" % (remaining, errors["n"]), file=sys.stderr)

    run_realtime(a, sim, send, flush)


def main(argv=None):
    a = parse_args(argv)
    sim = build_simulator(a)
    if a.dump:
        run_dump(a, sim)
    elif a.stdout:
        run_realtime(a, sim, lambda ev: print(json.dumps(ev, separators=(",", ":"))))
    else:
        run_kafka(a, sim)


if __name__ == "__main__":
    main()
