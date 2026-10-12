import pyarrow as pa

workload_schema = {}

workload_schema["pa_tasks_schema_required"] = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("start_time", pa.timestamp("ms"), nullable=False),
    pa.field("stop_time", pa.timestamp("ms"), nullable=False),
    pa.field("cpu_count", pa.int32(), nullable=False),
    pa.field("cpu_capacity", pa.float64(), nullable=False),
    pa.field("mem_capacity", pa.int64(), nullable=False),
])

workload_schema["pa_tasks_schema_optional"] = {}

workload_schema["pa_fragments_schema_required"] = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
    pa.field("duration", pa.int64(), nullable=False),
    pa.field("cpu_count", pa.int32(), nullable=False),
    pa.field("cpu_usage", pa.float64(), nullable=False),
])

workload_schema["pa_fragments_schema_optional"] = {}

workload_schema["tasks_name"] = "trace/meta"
workload_schema["fragments_name"] = "trace/trace"