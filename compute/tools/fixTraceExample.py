# %%

import pandas as pd

import os
from datetime import datetime 
from TraceTools.TraceWriter.writeTrace import writeTrace
from TraceTools.TraceWriter.variables import meta_columns, fragments_columns

from TraceTools.TraceFixingTools.fixTrace import fixTrace
from TraceTools.TraceFixingTools.fixStoppingTime import fixStoppingTime

# %%

trace_name = "2022-05-01_2022-11-30"

print("Loading trace")
df_tasks = pd.read_parquet(f"traces/marconi/source/tasks.parquet")
df_fragments = pd.read_parquet(f"traces/marconi/source/fragments.parquet")

# %%

print("fixing trace")
df_tasks_new, df_fragments_new = fixTrace(df_tasks, df_fragments)

# %%

print("saving trace")
if not os.path.exists(f"traces/marconi/fixed/{trace_name}"):
        os.makedirs(f"traces/marconi/fixed/{trace_name}") 

df_tasks_new.to_parquet(f"traces/marconi/fixed/tasks.parquet")
df_fragments_new.to_parquet(f"traces/marconi/fixed/fragments.parquet")
# %%
