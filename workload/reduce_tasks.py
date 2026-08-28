# %%

import pandas as pd 

import sys 

sys.path.append("..")

from workload.schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%


df_tasks = pd.read_parquet("traces/v5/solvinity-2025/tasks.parquet")
df_fragments = pd.read_parquet("traces/v5/solvinity-2025/fragments.parquet")

# %%

tasks = df_tasks["id"].unique()

# %%

print(len(tasks))


# %%

reduction_factor = 0.5

new_tasks = tasks[:int(len(tasks) * reduction_factor)]


# %%

print(len(new_tasks))


# %%

df_tasks_small = df_tasks[df_tasks["id"].isin(new_tasks)]
df_fragments_small = df_fragments[df_fragments["id"].isin(new_tasks)]


# %%


writeTrace(df_tasks_small, df_fragments_small, workload_schema, f"traces/v5/solvinity-2025-reduced-0.5")
# %%

# task_id = 11534573

# df_tasks_small = df_tasks[df_tasks["id"] == task_id]
# df_fragments_small = df_fragments[df_fragments["id"] == task_id]
# writeTrace(df_tasks_small, df_fragments_small, workload_schema, f"traces/v5/borg_{task_id}")
# %%

df_tasks = pd.read_parquet("traces/v5/borg/tasks.parquet")
df_fragments = pd.read_parquet("traces/v5/borg/fragments.parquet")

df_tasks["cpu_count"] = 1
df_tasks["cpu_capacity"] = 100
df_tasks["mem_capacity"] = 100000

writeTrace(df_tasks, df_fragments, workload_schema, f"traces/v5/borg")

