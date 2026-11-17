import json
import pandas as pd

rows = []
with open("4_spark_dropoff/dev/sample_session_end.jsonl") as f:
    for line in f:
        line = line.strip()
        if line:
            rows.append(json.loads(line))

df = pd.DataFrame(rows)
df["ts"] = pd.to_datetime(df["event_time"], utc=True)
df["window"] = df["ts"].dt.floor("5min")

sessions = df[df["event_type"] == "session_end"]
dropped = sessions[~sessions["completed"].astype(bool)].copy()
dropped["bucket"] = (dropped["video_position_sec"] // 30) * 30

print("BY POSITION:")
print(dropped.groupby(["window", "course_id", "module_id", "bucket"]).size().to_string())

mod = sessions.groupby(["window", "course_id", "module_id"]).agg(
    sessions_ended=("event_id", "count"),
    sessions_dropped=("completed", lambda s: (~s.astype(bool)).sum()))
mod["dropoff_rate"] = mod["sessions_dropped"] / mod["sessions_ended"]
print("\nBY MODULE:")
print(mod.sort_values("dropoff_rate", ascending=False).to_string())
