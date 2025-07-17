# %% 

import pandas as pd 

from schemas.workload_schema_1 import workload_schema
from write_workload_trace import writeTrace

# %%

start_time = pd.to_datetime("2024-01-01", utc=True)
start_time_ms = int(start_time.timestamp() * 1000)

# %%

df_tasks = pd.DataFrame([
    ["1", start_time_ms, 3_600_000, 1, 3_000, 10_000],
], columns=["id", "submission_time", "duration",
            "cpu_count", "cpu_capacity", "mem_capacity"])
    
df_fragments = pd.DataFrame([
    ["1", 3_600_000, 1, 2_500],
], columns=["id", "duration", "cpu_count", "cpu_usage"])

# %%

writeTrace(df_tasks, df_fragments, workload_schema, "traces/simple_workload")
