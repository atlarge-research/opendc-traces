# %% 

import pandas as pd 

from schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%

start_time = pd.to_datetime("2024-01-01", utc=True)
start_time_ms = int(start_time.timestamp() * 1000)

# %%

df_tasks = pd.DataFrame([
    [0, start_time_ms, 3_600_000, 1, 3_000, 10_000, [], [1, 2]],
    [1, start_time_ms, 3_600_000, 1, 3_000, 10_000, [0], [3]],
    [2, start_time_ms, 3_600_000, 1, 3_000, 10_000, [0], [3]],
    [3, start_time_ms, 3_600_000, 1, 3_000, 10_000, [1, 2], []],
], columns=["id", "submission_time", "duration",
            "cpu_count", "cpu_capacity", "mem_capacity", "parents", "children"])

df_fragments = pd.DataFrame([
    [0, 3_600_000, 2_500],
    [1, 3_600_000, 2_500],
    [2, 3_600_000, 2_500],
    [3, 3_600_000, 2_500],
], columns=["id", "duration", "cpu_usage"])

# %%

writeTrace(df_tasks, df_fragments, workload_schema, "traces/v5/workflow_example")

# %%
