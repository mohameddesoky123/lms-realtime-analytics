"""
Contract validator (Member 1).

Checks that a JSONL file follows the shared contract (project plan, Section 4).

  python validate_contract.py ../contracts/sample_clean.jsonl          # every line must be valid
  python validate_contract.py ../contracts/sample_raw.jsonl --raw      # report which kinds of dirt appear

Other members can use it too to check what they publish to lms_clean.
"""
import argparse
import json
import re
import sys
from collections import Counter

COMMON = ["event_id", "event_time", "user_id", "session_id", "course_id", "device", "region"]
EXTRA = {
    "session_start": [],
    "video_play": ["module_id", "video_position_sec"],
    "video_pause": ["module_id", "video_position_sec"],
    "video_seek": ["module_id", "video_position_sec"],
    "buffering": ["module_id", "video_position_sec", "buffer_duration_ms"],
    "quiz_submit": ["module_id", "quiz_score"],
    "lesson_navigate": ["module_id"],
    "session_end": ["module_id", "video_position_sec", "completed"],
}
COURSES = {"CS101", "DB201", "AI301", "NET210", "SE330"}
DEVICES = {"mobile", "desktop", "tablet", "smart_tv"}
REGIONS = {"cairo", "giza", "alexandria", "delta", "upper_egypt", "gulf", "europe"}
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def validate_event(ev, require_processed_at=True):
    """Returns a list of problems (empty list = valid clean event)."""
    p = []
    etype = ev.get("event_type")
    if etype not in EXTRA:
        return ["bad event_type: %r" % (etype,)]
    for f in COMMON + EXTRA[etype]:
        if ev.get(f) is None:
            p.append("missing %s" % f)
    if require_processed_at and ev.get("processed_at") is None:
        p.append("missing processed_at")
    t = ev.get("event_time")
    if t is not None and not (isinstance(t, str) and ISO_RE.match(t)):
        p.append("event_time not ISO-8601 UTC: %r" % (t,))
    if ev.get("course_id") is not None and ev["course_id"] not in COURSES:
        p.append("unknown course_id %r" % ev["course_id"])
    if ev.get("device") is not None and ev["device"] not in DEVICES:
        p.append("bad device %r" % ev["device"])
    if ev.get("region") is not None and ev["region"] not in REGIONS:
        p.append("bad region %r" % ev["region"])
    v = ev.get("video_position_sec")
    if v is not None and (not isinstance(v, int) or v < 0):
        p.append("video_position_sec out of range: %r" % (v,))
    v = ev.get("buffer_duration_ms")
    if v is not None and (not isinstance(v, int) or not 0 <= v <= 600000):
        p.append("buffer_duration_ms out of range: %r" % (v,))
    v = ev.get("quiz_score")
    if v is not None and not 0 <= v <= 100:
        p.append("quiz_score out of range: %r" % (v,))
    if ev.get("completed") is not None and not isinstance(ev["completed"], bool):
        p.append("completed must be boolean")
    return p


def classify_dirt(ev):
    """Returns the set of dirt labels found in a RAW event (for reporting only)."""
    d = set()
    if ev.get("user_id") is None or ev.get("course_id") is None:
        d.add("null_key")
    et = ev.get("event_type")
    if isinstance(et, str) and et != et.strip().lower():
        d.add("type_case")
    if not isinstance(ev.get("event_time"), str):
        d.add("epoch_time")
    for f in ("device", "region"):
        v = ev.get(f)
        if isinstance(v, str) and v != v.strip().lower():
            d.add("text_case")
    pos, buf, sc = ev.get("video_position_sec"), ev.get("buffer_duration_ms"), ev.get("quiz_score")
    if (pos is not None and pos < 0) or (buf is not None and buf > 600000) or (sc is not None and sc > 100):
        d.add("out_of_range")
    return d


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("file")
    ap.add_argument("--raw", action="store_true", help="report dirt types instead of requiring clean events")
    a = ap.parse_args()

    n = bad = 0
    types = Counter()
    dirt = Counter()
    ids = Counter()
    with open(a.file, encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            n += 1
            types[str(ev.get("event_type")).strip().lower()] += 1
            ids[ev.get("event_id")] += 1
            if a.raw:
                for k in classify_dirt(ev):
                    dirt[k] += 1
            else:
                probs = validate_event(ev)
                if probs:
                    bad += 1
                    if bad <= 10:
                        print("line %d: %s" % (ln, "; ".join(probs)))
    dups = sum(c - 1 for c in ids.values() if c > 1)
    print("lines: %d" % n)
    print("event types:", dict(types))
    if a.raw:
        dirt["duplicate_event_id"] = dups
        print("dirt found:", dict(dirt))
        return 0
    print("invalid lines: %d" % bad)
    print("duplicate event_ids: %d" % dups)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
