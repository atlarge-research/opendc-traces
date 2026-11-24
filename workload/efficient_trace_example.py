# %%

import pandas as pd
from utils.create_efficient_trace import create_efficient_trace

# %%

workload_name = "solvinity_6months"

df_tasks = pd.read_parquet(f"traces/v6_2/{workload_name}/tasks.parquet")
df_fragments = pd.read_parquet(f"traces/v6_2/{workload_name}/fragments.parquet")

create_efficient_trace(df_tasks, df_fragments, "v6_2", f"traces/v6_2/{workload_name}_efficient")

# %%
