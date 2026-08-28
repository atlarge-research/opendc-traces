# %% 

import pandas as pd 

import sys 

sys.path.append("..")

from workload.schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%

start_time = pd.to_datetime("2024-01-01T00:00:00", utc=True)
stop_time = pd.to_datetime("2024-02-01T00:00:00", utc=True)

start_time_ms = int(start_time.timestamp() * 1000)
stop_time_ms = int(stop_time.timestamp() * 1000)

# %%

num_tasks = 1000
num_fragments = 100000

tasks = []
fragments = []

task_duration = 3_600_000

fragment_duration = task_duration / num_fragments

print(fragment_duration)
    
import random

for i in range(num_tasks):
    start_time = random.randint(start_time_ms, stop_time_ms - task_duration)
    
    tasks.append([i, start_time, task_duration, 1, 1_000, 10_000])
    
    for _ in range(num_fragments):
        fragments.append([i, fragment_duration, 1, 1_000])

df_tasks = pd.DataFrame(
    tasks, columns=["id", "submission_time", "duration",
            "cpu_count", "cpu_capacity", "mem_capacity"]
)
    
df_fragments = pd.DataFrame(
    fragments, columns=["id", "duration", "cpu_count","cpu_usage"]
)

writeTrace(df_tasks, df_fragments, workload_schema, f"traces/v5/{num_fragments}_fragments")

# %%
