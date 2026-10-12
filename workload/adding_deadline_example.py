# %%

import pandas as pd

from utils.write_workload_trace import writeTrace

# %%

df_tasks = pd.read_parquet(f"traces/v6_2/surf/tasks.parquet")
df_fragments = pd.read_parquet(f"traces/v6_2/surf/fragments.parquet")

# %%

df_tasks["deferrable"] = True

# Create a deadline 24 hours after submission time
df_tasks["deadline"] = df_tasks["submission_time"] + 3600_000 * 24 

writeTrace(df_tasks, df_fragments, "v6_2", f"traces/v6_2/surf_deferrable")

# %%
