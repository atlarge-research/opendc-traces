# %% 

import pandas as pd 
import numpy as np 
import os

import pyarrow as pa
import pyarrow.parquet as pq

def writeTrace(df_meta, df_fragments, name):
    output_folder = f"traces/{name}"

    if not os.path.exists(f"{output_folder}/trace"):
        os.makedirs(f"{output_folder}/trace") 


    df_meta = df_meta[meta_columns]
    df_fragments = df_fragments[fragments_columns]

    pa_meta_out = pa.Table.from_pandas(
        df = df_meta,
        schema = pa_schema_meta,
        preserve_index=False
    )

    pq.write_table(pa_meta_out, f"traces/{name}/trace/meta.parquet")

    pa_fragments_out = pa.Table.from_pandas(
        df = df_fragments,
        schema = pa_schema_fragments,
        preserve_index=False
    )

    pq.write_table(pa_fragments_out, f"traces/{name}/trace/trace.parquet")


meta_columns = ["id", "start_time", "stop_time", "cpu_count", "cpu_capacity", "mem_capacity"]
schema_meta = {
    "id": pa.string(),
    "start_time": pa.timestamp("ms"),
    "stop_time": pa.timestamp("ms"),
    "cpu_count": pa.int32(),
    "cpu_capacity": pa.float64(),
    "mem_capacity": pa.int64()
}

pa_schema_meta = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_meta.items()])


fragments_columns = ["id", "timestamp", "duration", "cpu_count", "cpu_usage"]
schema_fragments = {
    "id": pa.string(),
    "timestamp": pa.timestamp("ms"),
    "duration": pa.int64(),
    "cpu_count": pa.int32(),
    "cpu_usage": pa.float64()
}

pa_schema_fragments = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_fragments.items()])

# %%

df_tasks = pd.read_parquet("traces/Galaxy_source/tasks/schema-1.0/part.0.parquet")

# %%

expected_cpu_utilization = 0.5
max_cpu_usage = 3300 # 3.3 GHz

df_tasks["start_time"] = pd.to_datetime(df_tasks.ts_submit, unit="ms")
df_tasks["id"] = df_tasks["id"].astype(str)
df_tasks['stop_time'] = df_tasks['start_time'] + pd.to_timedelta(df_tasks['runtime'], unit='ms')
df_tasks['cpu_count'] = df_tasks['resource_amount_requested'].astype(int)
df_tasks['cpu_capacity'] = max_cpu_usage * expected_cpu_utilization
df_tasks['mem_capacity'] = df_tasks['memory_requested'].replace(-1, 10_000).astype(int)

# %%

df_tasks.loc[df_tasks['cpu_count'] < 1, 'cpu_count'] = 1

# %%

df_tasks.cpu_count.min()


# %%

df_meta = df_tasks[meta_columns]
df_fragments = df_meta.copy()

# %%

df_fragments['timestamp'] = df_fragments["stop_time"]
df_fragments["duration"] = ((df_fragments["stop_time"] - df_fragments["start_time"]).dt.total_seconds() * 1000).astype(int)
df_fragments["cpu_usage"] = df_fragments["cpu_capacity"]

# %%

# Add a delay, so the workload does not start at 1970-01-01
delay = pd.Timestamp("2022-01-07") - pd.Timestamp("1970-01-01")

df_meta.start_time = df_meta.start_time + delay
df_meta.stop_time = df_meta.stop_time + delay
df_fragments.timestamp = df_fragments.timestamp + delay

# %%

writeTrace(df_meta, df_fragments, "Galaxy")

# %%



