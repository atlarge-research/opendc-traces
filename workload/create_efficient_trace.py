# %%

import pandas as pd 

import sys 

sys.path.append("..")

from workload.schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%

df_tasks = pd.read_parquet("traces/v5/borg/tasks.parquet")
df_fragments = pd.read_parquet("traces/v5/borg/fragments.parquet")

# %%

df_tasks = df_tasks.sort_values(by=["submission_time"])
ids = list(df_tasks.id)

# %%

df_fragments["id"] = pd.Categorical(df_fragments["id"], categories=ids, ordered=True)
df_fragments.sort_values(by=["id"], inplace=True)

df_fragments


# %%

# Remove empty fragments
df_fragments = df_fragments[df_fragments["duration"] > 0]

# %%

fragment_counts = df_fragments.groupby("id").size()
df_tasks["num_fragments"] = df_tasks["id"].map(fragment_counts).fillna(0).astype(int)

# %%

writeTrace(df_tasks, df_fragments, workload_schema, f"traces/v5/borg_efficient")
# %%

print("t")
# %%
