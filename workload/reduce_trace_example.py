# %%

import pandas as pd 

from utils.write_workload_trace import writeTrace
from utils.reduce_trace import reduce_tasks, reduce_duration

# %%

df_tasks = pd.read_parquet(f"traces/v6_2/surf/tasks.parquet")
df_fragments = pd.read_parquet(f"traces/v6_2/surf/fragments.parquet")

# %%

df_tasks_half, df_fragments_half = reduce_tasks(df_tasks, df_fragments, 0.5)
writeTrace(df_tasks_half, df_fragments_half, "v6_2", f"traces/v6_2/surf_half")

# %%

df_tasks_week, df_fragments_week = reduce_duration(df_tasks, df_fragments, 3600_000 * 24 * 7)
writeTrace(df_tasks_week, df_fragments_week, "v6_2", f"traces/v6_2/surf_week")

