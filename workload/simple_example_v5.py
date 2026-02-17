# %% 

import pandas as pd 

import sys 

sys.path.append("..")

from workload.schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%

start_time = pd.to_datetime("2024-01-01T00:00:00", utc=True)
stop_time = pd.to_datetime("2024-01-01T01:00:00", utc=True)

start_time_ms = int(start_time.timestamp() * 1000)
stop_time_ms = int(stop_time.timestamp() * 1000)

# %%

df_tasks = pd.DataFrame([
    [1, start_time_ms, 3_600_000, 1, 1_000, 10_000],
], columns=["id", "submission_time", "duration",
            "cpu_count", "cpu_capacity", "mem_capacity"])
    
df_fragments = pd.DataFrame([
    [1, 1_800_000, 1, 0.0],
    [1, 1_800_000, 1, 1000.0],
], columns=["id", "duration", "cpu_count","cpu_usage"])

# %%

writeTrace(df_tasks, df_fragments, workload_schema, "traces/v5/test_0_1")

# %%
