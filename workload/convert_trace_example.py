# %%

from utils.converters import convert
from utils.write_workload_trace import writeTrace
import pandas as pd

# %%

trace_name = "bitbrains-small"

df_tasks = pd.read_parquet(f"traces/v6_2/{trace_name}/tasks.parquet")
df_fragments = pd.read_parquet(f"traces/v6_2/{trace_name}/fragments.parquet")

# %%

VERSIONS = ["v1", "v2", "v3", "v3_1", "v4", "v5", "v5_1", "v6", "v6_1", "v6_2"]

for i in range(len(VERSIONS) - 1)[::-1]:
    version_1 = VERSIONS[i + 1]
    version_2 = VERSIONS[i]

    print(f"Converting and writing {version_1} to {version_2}...")

    df_tasks, df_fragments = convert(df_tasks, df_fragments, version_1, version_2)
    writeTrace(df_tasks, df_fragments, version_2, f"traces/{version_2}/{trace_name}")

# version_1 = "v6_1"
# version_2 = "v6"

# df_tasks, df_fragments = convert(df_tasks, df_fragments, version_1, version_2)
# writeTrace(df_tasks, df_fragments, version_2, f"traces/{version_2}/{trace_name}")

# %%
