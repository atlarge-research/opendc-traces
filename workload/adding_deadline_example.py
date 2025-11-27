# %%

import pandas as pd

from schemas.workload_schema_v5 import workload_schema as workload_schema_v5

from utils.write_workload_trace import writeTrace
# %%

df_tasks = pd.read_parquet(f"traces/v5/simple_workload/tasks.parquet")
df_fragments = pd.read_parquet(f"traces/v5/simple_workload/fragments.parquet")

# %%

df_tasks["deferrable"] = True

# Create a deadline 24 hours after submission time
df_tasks["deadline"] = df_tasks["submission_time"] + 86400000 

writeTrace(df_tasks, df_fragments, workload_schema_v5, f"traces/v5/simple_workload_deadline")

# %%
