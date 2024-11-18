# %% 

import pandas as pd 

from TraceTools.TraceWriter.writeTrace import writeTrace
from TraceTools.TraceWriter.variables import meta_columns, fragments_columns
from datetime import datetime 


# %%

# create meta
meta = [
    ["0", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 2, 12345, 100_000,],
    ["1", datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 15:00", "%Y-%m-%d %H:%M"), 2, 12345, 100_000,],
]

df_meta = pd.DataFrame(meta, columns=meta_columns)

# create fragment
trace = [
    ["0", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 2, 12345],
    ["0", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 60*60*1000, 2, 12345],
    ["1", datetime.strptime("2024-2-1 14:00", "%Y-%m-%d %H:%M"), 60*60*1000, 2, 12345],
    ["1", datetime.strptime("2024-2-1 15:00", "%Y-%m-%d %H:%M"), 60*60*1000, 2, 12345],
]

df_trace = pd.DataFrame(trace, columns=fragments_columns)

writeTrace(df_meta, df_trace, "single_task")

# %%


df_meta = pd.read_parquet("traces/bitbrains-small/trace/meta.parquet")
df_trace = pd.read_parquet("traces/bitbrains-small/trace/trace.parquet")

# %%

df_meta.start_time.value_counts()


task_list = df_meta[df_meta.start_time == df_meta.start_time.min()]["id"]

# %%

# task_list = ["1019",
# "1023",
# "1026",
# "1052",
# "1073",
# "1129",
# "1132",
# "1138",
# "1147",
# "1152",]


df_meta = df_meta[df_meta.id.isin(task_list)]

# %%

df_trace = df_trace[df_trace.id.isin(task_list)]

# %%

len(df_trace.id.unique())

# %%

writeTrace(df_meta, df_trace, "bitbrains-reduced")

# %%
df_meta.start_time.value_counts()