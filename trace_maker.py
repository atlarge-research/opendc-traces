# %%

import pandas as pd
from datetime import datetime 
import pyarrow as pa
import pyarrow.parquet as pq

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


trace_columns = ["id", "timestamp", "duration", "cpu_count", "cpu_usage"]
schema_trace = {
    "id": pa.string(),
    "timestamp": pa.timestamp("ms"),
    "duration": pa.int64(),
    "cpu_count": pa.int32(),
    "cpu_usage": pa.float64()
}

pa_schema_trace = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_trace.items()])

# %%

start_date = datetime.strptime("2024-2-1", "%Y-%m-%d")
end_date = datetime.strptime("2024-2-2", "%Y-%m-%d")
output_folder = "traces/incorrect"

if not os.path.exists(f"{output_folder}/trace"):
    os.makedirs(f"{output_folder}/trace")

# create meta
meta = [
    ["correct", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["missing", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["overlap", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["overlap2", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["start", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["stop", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
]

df_meta = pd.DataFrame(meta, columns=meta_columns)

pa_meta_out = pa.Table.from_pandas(
    df = df_meta,
    schema = pa_schema_meta,
    preserve_index=False
)

pq.write_table(pa_meta_out, f"{output_folder}/trace/meta.parquet")

# create fragment
trace = [
    ["correct", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 1000],
    ["correct", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 2000],

    ["missing", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 2000],
    ["missing", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), (60*60-1)*1000, 1, 3000],

    ["overlap", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 3000],
    ["overlap", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 4000],
    ["overlap", datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 4000],

    ["overlap2", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 5000],
    ["overlap2", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 6000],
    ["overlap2", datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 7000],

    ["start", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), (60*60 - 1)*1000, 1, 4000],
    ["start", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 5000],

    ["stop", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 5000],
    ["stop", datetime.strptime("2024-2-1 12:01", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 6000],
]

df_trace = pd.DataFrame(trace, columns=trace_columns)

pa_trace_out = pa.Table.from_pandas(
    df = df_trace,
    schema = pa_schema_trace,
    preserve_index=False
)

pq.write_table(pa_trace_out, f"{output_folder}/trace/trace.parquet")


# %%
