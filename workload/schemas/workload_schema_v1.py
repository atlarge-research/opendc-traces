import pyarrow as pa

workload_schema = {}

workload_schema["pa_tasks_schema_required"] = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("submissionTime", pa.timestamp("ms"), nullable=False),
    pa.field("endTime", pa.timestamp("ms"), nullable=False),
    pa.field("maxCores", pa.int32(), nullable=False),
    pa.field("requiredMemory", pa.int64(), nullable=False),
])

workload_schema["pa_tasks_schema_optional"] = {}

workload_schema["pa_fragments_schema_required"] = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("time", pa.timestamp("ms"), nullable=False),
    pa.field("duration", pa.int64(), nullable=False),
    pa.field("cores", pa.int32(), nullable=False),
    pa.field("cpuUsage", pa.float64(), nullable=False),
    pa.field("flops", pa.int64(), nullable=False),
])

workload_schema["pa_fragments_schema_optional"] = {}

workload_schema["tasks_name"] = "trace/meta"
workload_schema["fragments_name"] = "trace/trace"