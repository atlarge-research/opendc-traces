# %% 

import pandas as pd 

import sys 

sys.path.append("..")

from utils.write_workload_trace import writeTrace

# %%

start_time = pd.to_datetime("2024-01-01T00:00:00", utc=True)
duration = pd.to_timedelta("1h")
stop_time = start_time + duration

start_time_ms = int(start_time.timestamp() * 1000)
stop_time_ms = int(stop_time.timestamp() * 1000)

# %%

df_tasks = pd.DataFrame([
    [1, start_time, duration, 1, 1_000, 10_000],
], columns=["id", "submission_time", "duration",
            "cpu_count", "cpu_capacity", "mem_capacity"])
    
df_fragments = pd.DataFrame([
    [1, duration, 1_000],
], columns=["id", "duration","cpu_usage"])

# %%

writeTrace(df_tasks, df_fragments, "v6_2", "traces/v6_2/simple_workload")

# %%
