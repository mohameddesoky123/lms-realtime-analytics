"""Tests for the simulator. Run from the 1_producer folder:  pytest -q"""
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator import Simulator, load_config, parse_iso          # noqa: E402
from validate_contract import validate_event, classify_dirt, EXTRA   # noqa: E402

START = 1794650400.0   # fixed virtual start so tests are deterministic


def make(n, **kw):
    kw.setdefault("users", 300)
    kw.setdefault("rate", 40)
    kw.setdefault("seed", 123)
    kw.setdefault("start", START)
    sim = Simulator(load_config(), **kw)
    out = []
    for due, ev in sim.stream():
        out.append((due, ev))
        if len(out) >= n:
            break
    return out


def test_clean_events_follow_the_contract():
    events = make(5000, clean=True)
    bad = [(ev, validate_event(ev)) for _, ev in events if validate_event(ev)]
    assert not bad, bad[:3]


def test_all_event_types_appear():
    types = Counter(ev["event_type"] for _, ev in make(8000, clean=True))
    assert set(types) == set(EXTRA), types


def test_clean_mode_has_no_dirt_and_unique_ids():
    events = [ev for _, ev in make(5000, clean=True)]
    assert all(not classify_dirt(ev) for ev in events)
    ids = [ev["event_id"] for ev in events]
    assert len(ids) == len(set(ids))


def test_dirty_rates_are_close_to_the_table():
    events = [ev for _, ev in make(40000, late_rate=0.0)]
    n = len(events)
    dirt = Counter()
    for ev in events:
        for k in classify_dirt(ev):
            dirt[k] += 1
    ids = Counter(ev["event_id"] for ev in events)
    dups = sum(c - 1 for c in ids.values() if c > 1)
    assert 0.020 <= dirt["null_key"] / n <= 0.040
    assert 0.035 <= dirt["type_case"] / n <= 0.065
    assert 0.035 <= dirt["epoch_time"] / n <= 0.065
    assert 0.035 <= dirt["text_case"] / n <= 0.075      # device and region can both be hit
    assert 0.020 <= dups / n <= 0.040
    assert 0.002 <= dirt["out_of_range"] / n <= 0.025   # only events that carry a numeric field can be hit


def test_dirty_rate_zero_means_no_errors():
    events = [ev for _, ev in make(4000, dirty_scale=0.0)]
    assert all(not classify_dirt(ev) for ev in events)


def test_late_events_rate_and_size():
    events = make(20000, clean=True, late_rate=0.02)
    late = [due - parse_iso(ev["event_time"]) for due, ev in events if due - parse_iso(ev["event_time"]) > 5]
    assert 0.010 <= len(late) / len(events) <= 0.035
    assert all(29.9 <= x <= 90.1 for x in late)


def test_planted_dropoff_pattern_cs101_m03():
    events = [ev for _, ev in make(60000, clean=True)]
    ended = defaultdict(lambda: [0, 0])          # module -> [ended, dropped]
    positions = []
    for ev in events:
        if ev["event_type"] == "session_end":
            ended[ev["module_id"]][0] += 1
            if ev["completed"] is False:
                ended[ev["module_id"]][1] += 1
                if ev["module_id"] == "CS101-M03":
                    positions.append(ev["video_position_sec"])
    rate = {m: d / e for m, (e, d) in ended.items() if e >= 20}
    target = rate["CS101-M03"]
    others = [r for m, r in rate.items() if m != "CS101-M03"]
    assert target > 0.6, rate
    assert target > 1.5 * (sum(others) / len(others))
    inside = sum(1 for p in positions if 210 <= p <= 270) / len(positions)
    assert inside > 0.7


def test_planted_buffering_pattern_mobile_upper_egypt():
    events = [ev for _, ev in make(60000, clean=True)]
    groups = defaultdict(list)
    for ev in events:
        if ev["event_type"] == "buffering":
            groups[(ev["device"], ev["region"])].append(ev["buffer_duration_ms"])
    slow = groups[("mobile", "upper_egypt")]
    assert len(slow) > 30
    assert sum(slow) / len(slow) > 3000
    for k, v in groups.items():
        if k != ("mobile", "upper_egypt") and len(v) >= 20:
            assert sum(v) / len(v) < 3000, k


def test_same_seed_gives_same_data():
    a = [ev for _, ev in make(1500, seed=7)]
    b = [ev for _, ev in make(1500, seed=7)]
    c = [ev for _, ev in make(1500, seed=8)]
    assert a == b
    assert a != c


def test_events_come_in_time_order():
    dues = [due for due, _ in make(5000)]
    assert dues == sorted(dues)


def test_user_has_a_fixed_device_and_region():
    seen = {}
    for _, ev in make(8000, clean=True):
        key = ev["user_id"]
        val = (ev["device"], ev["region"])
        assert seen.setdefault(key, val) == val
