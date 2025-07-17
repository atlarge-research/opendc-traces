# %% 

import pandas as pd
from datetime import datetime 
import os

import pyarrow as pa
import pyarrow.parquet as pq

tasks_columns = ["id", "submission_time", "duration", "cpu_count", "cpu_capacity", "mem_capacity"]
schema_tasks = {
    "id": pa.string(),
    "submission_time": pa.timestamp("ms"),
    "duration": pa.int64(),
    "cpu_count": pa.int32(),
    "cpu_capacity": pa.float64(),
    "mem_capacity": pa.int64()
}

pa_schema_tasks = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_tasks.items()])


fragments_columns = ["id", "duration", "cpu_count", "cpu_usage"]
schema_fragments = {
    "id": pa.string(),
    "duration": pa.int64(),
    "cpu_count": pa.int32(),
    "cpu_usage": pa.float64()
}

pa_schema_fragments = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_fragments.items()])

def writeTrace(df_tasks, df_fragments, output_folder):
    if not os.path.exists(f"{output_folder}"):
        os.makedirs(f"{output_folder}") 


    df_tasks = df_tasks[tasks_columns]
    df_fragments = df_fragments[fragments_columns]

    pa_tasks_out = pa.Table.from_pandas(
        df = df_tasks,
        schema = pa_schema_tasks,
        preserve_index=False
    )

    pq.write_table(pa_tasks_out, f"{output_folder}/tasks.parquet")

    pa_fragments_out = pa.Table.from_pandas(
        df = df_fragments,
        schema = pa_schema_fragments,
        preserve_index=False
    )

    pq.write_table(pa_fragments_out, f"{output_folder}/fragments.parquet")

