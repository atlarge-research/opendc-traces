# %%

import pandas as pd 

import sys 

sys.path.append("..")

from workload.schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%

df_tasks_base = pd.read_parquet("traces/v5/solvinity/tasks.parquet")
df_fragments_base = pd.read_parquet("traces/v5/solvinity/fragments.parquet")
num_tasks = 100


# %%
# Correct id

ids = df_tasks_base["id"].unique()[:num_tasks]
df_tasks = df_tasks_base[df_tasks_base["id"].isin(ids)]
df_fragments = df_fragments_base[df_fragments_base["id"].isin(ids)]


# %%

writeTrace(df_tasks, df_fragments, workload_schema, f"traces/v5/solvinity_small")
# %%

df_tasks_base.id.nunique()
# %%
