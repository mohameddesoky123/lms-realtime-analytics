"""
LMS event simulator (Member 1).

Simulates many virtual learners using an online learning platform and produces
events that follow the shared contract (project plan, Section 4).

Design
------
* Every virtual learner is a small state machine (class Session).
* Sessions are scheduled with a heap (priority queue) ordered by the time of
  their next event.
* The simulator runs on a *virtual clock*:
    - for Kafka / stdout the producer sleeps until each event is due (real time)
    - for --dump the producer does NOT sleep (fast generation of sample files)
  so both modes use exactly the same code.

The only public entry point is Simulator.stream(), a generator of
(due_time, event_dict) pairs in time order.
"""
import heapq
import math
import os
import random
from datetime import datetime, timezone

import yaml

EVENT_FIELDS = [
    "event_id", "event_time", "user_id", "session_id", "course_id", "module_id",
    "event_type", "video_position_sec", "buffer_duration_ms", "quiz_score",
    "completed", "device", "region",
]

EVENT_TYPES = [
    "session_start", "video_play", "video_pause", "video_seek",
    "buffering", "quiz_submit", "lesson_navigate", "session_end",
]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def iso(ts):
    """Epoch seconds (float) -> ISO-8601 UTC string with milliseconds, e.g. 2026-11-14T10:05:03.120Z"""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (dt.microsecond // 1000)


def parse_iso(text):
    """ISO string ending in Z -> epoch seconds."""
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp() \
        if "." in text else \
        datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


def load_config(path=None):
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def weighted_choice(rng, weights):
    keys = list(weights.keys())
    return rng.choices(keys, weights=[weights[k] for k in keys], k=1)[0]


# --------------------------------------------------------------------------
# one virtual learner
# --------------------------------------------------------------------------
class Session:
    """State machine of one learning session.

    step(now) returns (list_of_event_specs, delay_seconds_or_None).
    Each event spec is (event_type, extra_fields_dict). delay None = session finished.
    """

    def __init__(self, sim, user_id, session_id, device, region, course, module, modules_to_watch):
        self.sim = sim
        self.rng = sim.rng
        self.user_id = user_id
        self.session_id = session_id
        self.device = device
        self.region = region
        self.course = course
        self.module = module              # current module number (1-based)
        self.modules_left = modules_to_watch
        self.state = "START"
        self.pos = 0
        self.length = 0

    # -- helpers ---------------------------------------------------------
    @property
    def module_id(self):
        return "%s-M%02d" % (self.course, self.module)

    def _delay(self):
        lo, hi = self.sim.cfg["traffic"]["tick_delay_sec"]
        return self.rng.uniform(lo, hi)

    def _dropoff_prob(self):
        b = self.sim.cfg["behavior"]
        p = b["base_dropoff_per_tick"]
        pat = b["pattern_dropoff"]
        if (self.course == pat["course"] and self.module == pat["module"]
                and pat["from_sec"] <= self.pos <= pat["to_sec"]):
            p = max(p, pat["per_tick"])
        return p

    def _buffer_event(self):
        """Returns buffer duration in ms or None (no buffering on this tick)."""
        bc = self.sim.cfg["buffering"]
        prob = bc["base_prob_per_tick"]
        median = bc["lognormal_median_ms"]
        slow = bc["slow_group"]
        if self.device == slow["device"] and self.region == slow["region"]:
            prob *= slow["prob_multiplier"]
            median *= slow["duration_multiplier"]
        if self.rng.random() >= prob:
            return None
        ms = self.rng.lognormvariate(math.log(median), bc["lognormal_sigma"])
        return int(min(max(ms, 100), 600000))

    def _end(self, completed):
        return [("session_end", {"module_id": self.module_id,
                                 "video_position_sec": int(self.pos),
                                 "completed": completed})], None

    # -- state machine ---------------------------------------------------
    def step(self, now):
        cfg = self.sim.cfg
        b = cfg["behavior"]
        t = cfg["traffic"]
        rng = self.rng

        if self.state == "START":
            self.state = "PLAY"
            return [("session_start", {})], self._delay()

        if self.state == "PLAY":                       # start the video of the current module
            self.pos = 0
            self.length = rng.randint(*t["video_length_sec"])
            self.state = "TICK"
            return [("video_play", {"module_id": self.module_id, "video_position_sec": 0})], self._delay()

        if self.state == "RESUME":                     # after a pause
            self.state = "TICK"
            return [("video_play", {"module_id": self.module_id,
                                    "video_position_sec": int(self.pos)})], self._delay()

        if self.state == "TICK":                       # the learner keeps watching
            self.pos += rng.randint(*t["position_step_sec"])
            if self.pos >= self.length:
                self.pos = self.length
                self.state = "FINISH"
                return [], self._delay()
            if rng.random() < self._dropoff_prob():    # learner leaves -> drop-off
                return self._end(False)
            ms = self._buffer_event()
            if ms is not None:
                return [("buffering", {"module_id": self.module_id,
                                       "video_position_sec": int(self.pos),
                                       "buffer_duration_ms": ms})], self._delay()
            r = rng.random()
            if r < b["pause_prob"]:
                self.state = "RESUME"
                return [("video_pause", {"module_id": self.module_id,
                                         "video_position_sec": int(self.pos)})], rng.uniform(2, 8)
            if r < b["pause_prob"] + b["seek_prob"]:
                self.pos = int(min(max(0, self.pos + rng.choice([-1, 1]) * rng.randint(20, 90)),
                                   self.length - 1))
                return [("video_seek", {"module_id": self.module_id,
                                        "video_position_sec": int(self.pos)})], self._delay()
            return [], self._delay()                   # quiet tick: nothing happened

        if self.state == "FINISH":                     # module finished
            self.state = "NAV"
            if rng.random() < b["quiz_prob"]:
                score = min(max(rng.gauss(70, 15), 0), 100)
                return [("quiz_submit", {"module_id": self.module_id,
                                         "quiz_score": round(score, 1)})], self._delay()
            return [], 0.2

        if self.state == "NAV":                        # next module or finish the session
            self.modules_left -= 1
            if self.modules_left <= 0 or self.module >= cfg["modules_per_course"]:
                return self._end(True)
            self.module += 1
            self.state = "PLAY"
            return [("lesson_navigate", {"module_id": self.module_id})], self._delay()

        raise RuntimeError("unknown state " + self.state)


# --------------------------------------------------------------------------
# the simulator
# --------------------------------------------------------------------------
class Simulator:
    def __init__(self, cfg, users=500, rate=50.0, seed=None, start=None,
                 clean=False, dirty_scale=1.0, late_rate=None):
        self.cfg = cfg
        self.users = max(1, int(users))
        self.rate = float(rate)
        self.rng = random.Random(seed)
        self.t0 = float(start) if start is not None else __import__("time").time()
        self.clean = clean
        self.dirty_scale = 0.0 if clean else float(dirty_scale)
        self.late_rate = cfg["late"]["rate"] if late_rate is None else float(late_rate)

        # fixed profile per user (device + region), created lazily
        self._profiles = {}
        self.heap = []
        self._counter = 0
        self.active = set()          # active Session objects
        self.busy = set()            # user numbers with a running session
        self._session_seq = 0
        # online estimate of events per session-second (used to hit the target rate)
        self._n_events = 0
        self._area = 0.0
        self._last_t = self.t0

    # -- profiles --------------------------------------------------------
    def _profile(self, n):
        if n not in self._profiles:
            self._profiles[n] = (weighted_choice(self.rng, self.cfg["devices"]),
                                 weighted_choice(self.rng, self.cfg["regions"]))
        return self._profiles[n]

    # -- scheduling ------------------------------------------------------
    def _push(self, due, session):
        self._counter += 1
        heapq.heappush(self.heap, (due, self._counter, session))

    def _target_sessions(self, now):
        t = self.cfg["traffic"]
        wave = 1.0 + t["wave_amplitude"] * math.sin(2 * math.pi * (now - self.t0) / t["wave_period_sec"])
        eps = (self._n_events + 5.0) / (self._area + 10.0)      # events per session-second
        target = int(round(self.rate * wave / eps))
        return max(1, min(self.users, target))

    def _spawn(self, now):
        idle = [n for n in range(1, self.users + 1) if n not in self.busy]
        if not idle:
            return
        n = self.rng.choice(idle) if len(idle) < 50 else self.rng.randint(1, self.users)
        if n in self.busy:
            return
        device, region = self._profile(n)
        self._session_seq += 1
        b = self.cfg["behavior"]
        course = self.rng.choice(self.cfg["courses"])
        module = self.rng.randint(1, b["start_module_max"])
        watch = self.rng.randint(1, b["max_modules_per_session"])
        s = Session(self, "u%d" % (1000 + n), "s-%d" % (5000 + self._session_seq),
                    device, region, course, module, watch)
        self.busy.add(n)
        self.active.add(s)
        self._push(now + self.rng.uniform(0, 3), s)

    def _rebalance(self, now):
        target = self._target_sessions(now)
        guard = 0
        while len(self.active) < target and guard < 5000:
            before = len(self.active)
            self._spawn(now)
            guard += 1
            if len(self.active) == before and len(self.busy) >= self.users:
                break

    # -- event construction ----------------------------------------------
    def _build(self, session, etype, extra, due):
        late = self.rng.random() < self.late_rate
        ev_time = due
        if late:
            ev_time = due - self.rng.uniform(self.cfg["late"]["min_sec"], self.cfg["late"]["max_sec"])
        ev = {
            "event_id": "e-%08x" % self.rng.getrandbits(32),
            "event_time": iso(ev_time),
            "user_id": session.user_id,
            "session_id": session.session_id,
            "course_id": session.course,
            "module_id": None,
            "event_type": etype,
            "video_position_sec": None,
            "buffer_duration_ms": None,
            "quiz_score": None,
            "completed": None,
            "device": session.device,
            "region": session.region,
        }
        ev.update(extra)
        return ev

    # -- dirty data --------------------------------------------------------
    def _make_dirty(self, ev):
        """Returns a list with 1 event (or 2 when duplicated). Rates from config['dirty'] * dirty_scale."""
        d = self.cfg["dirty"]
        s = self.dirty_scale
        r = self.rng
        ev = dict(ev)
        if r.random() < d["null_key"] * s:
            ev[r.choice(["user_id", "course_id"])] = None
        if r.random() < d["type_case"] * s:
            ev["event_type"] = r.choice([ev["event_type"].upper(), " %s " % ev["event_type"],
                                         ev["event_type"].title()])
        if r.random() < d["epoch_time"] * s:
            ev["event_time"] = int(round(parse_iso(ev["event_time"]) * 1000))
        if r.random() < d["text_case"] * s:
            field = r.choice(["device", "region"])
            v = ev[field]
            ev[field] = r.choice([v.upper(), v.title(), " %s " % v])
        if r.random() < d["out_of_range"] * s:
            options = []
            if ev["video_position_sec"] is not None:
                options.append(("video_position_sec", -r.randint(1, 30)))
            if ev["buffer_duration_ms"] is not None:
                options.append(("buffer_duration_ms", r.randint(700000, 2000000)))
            if ev["quiz_score"] is not None:
                options.append(("quiz_score", round(r.uniform(101, 250), 1)))
            if options:
                k, v = r.choice(options)
                ev[k] = v
        out = [ev]
        if r.random() < d["duplicate"] * s:
            out.append(dict(ev))                       # exact copy: same event_id
        return out

    # -- public ------------------------------------------------------------
    def stream(self):
        """Generator of (due_time, event_dict) in time order. Never ends by itself."""
        self._rebalance(self.t0)
        while self.heap:
            due, _, sess = heapq.heappop(self.heap)
            self._area += len(self.active) * (due - self._last_t)
            self._last_t = due

            specs, delay = sess.step(due)
            if delay is None:
                self.active.discard(sess)
                self.busy.discard(int(sess.user_id[1:]) - 1000)
            else:
                self._push(due + delay, sess)

            for etype, extra in specs:
                ev = self._build(sess, etype, extra, due)
                self._n_events += 1
                if self.clean:
                    ev["processed_at"] = iso(due + self.rng.uniform(0.2, 1.5))
                    batch = [ev]
                else:
                    batch = self._make_dirty(ev)
                for e in batch:
                    yield due, e

            self._rebalance(due)
            if not self.heap:                          # safety: never run dry
                self._spawn(due)
