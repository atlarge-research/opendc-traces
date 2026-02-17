# %%

import pandas as pd 

import sys 

sys.path.append("..")

from workload.schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%

df_tasks = pd.read_parquet("/home/dante-niewenhuis/Documents/opendc/Traces/github/workload/traces/v5/borg/tasks.parquet")
df_fragments = pd.read_parquet("/home/dante-niewenhuis/Documents/opendc/Traces/github/workload/traces/v5/borg/fragments.parquet")

# %%

max_shifting = 24 * 3600 * 1000 # 24 hours in milliseconds

df_tasks["deadline"] = df_tasks["submission_time"] + max_shifting

# %%

df_tasks["deferrable"] = True

# %%

writeTrace(df_tasks, df_fragments, workload_schema, "traces/v5/borg_shifting_workload")
# %%
