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


fragments_columns = ["id", "timestamp", "duration", "cpu_count", "cpu_usage"]
schema_fragments = {
    "id": pa.string(),
    "timestamp": pa.timestamp("ms"),
    "duration": pa.int64(),
    "cpu_count": pa.int32(),
    "cpu_usage": pa.float64()
}

pa_schema_fragments = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_fragments.items()])
