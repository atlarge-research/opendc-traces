# %%

import pandas as pd 
from TraceTools.TraceWriter.variables import meta_columns, fragments_columns
from TraceTools.TraceWriter.writeTrace import writeTrace

# %%

traceName = "LANL_Trinity"

pathToTraceSource = f"traces/{traceName}_source"

expected_cpu_utilization = 0.5
max_cpu_usage = 3300 # 3.3 GHz
cpu_usage = max_cpu_usage * expected_cpu_utilization


df_tasks = pd.read_parquet(f"{pathToTraceSource}/tasks/schema-1.0/part.0.parquet")

# %% Update the tasks dataframe
df_tasks["start_time"] = pd.to_datetime(df_tasks.ts_submit, unit="ms")
df_tasks["id"] = df_tasks["id"].astype(str)
df_tasks['stop_time'] = df_tasks['start_time'] + pd.to_timedelta(df_tasks['runtime'], unit='ms')
df_tasks['cpu_count'] = df_tasks['resource_amount_requested'].astype(int)
df_tasks['cpu_capacity'] = cpu_usage
df_tasks['mem_capacity'] = df_tasks['memory_requested'].replace(-1, 10_000).astype(int)

# Set all missing cpu_counts to 1
df_tasks.loc[df_tasks['cpu_count'] < 1, 'cpu_count'] = 1

# %%

# df_meta can be created by taking the required columns from df_tasks
df_meta = df_tasks[meta_columns]

# Create a single fragment for each task that is of equal size of the full task
df_fragments = df_meta.copy()

# %%

df_fragments['timestamp'] = df_fragments["stop_time"]
df_fragments["duration"] = ((df_fragments["stop_time"] - df_fragments["start_time"]).dt.total_seconds() * 1000).astype(int)
df_fragments["cpu_usage"] = df_fragments["cpu_capacity"]

writeTrace(df_meta, df_fragments, traceName)
