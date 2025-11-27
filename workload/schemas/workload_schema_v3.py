import pyarrow as pa

workload_schema = {}

workload_schema["pa_tasks_schema_required"] = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("submission_time", pa.int64(), nullable=False),
    pa.field("duration", pa.int64(), nullable=False),
    pa.field("cpu_count", pa.int32(), nullable=False),
    pa.field("cpu_capacity", pa.float64(), nullable=False),
    pa.field("mem_capacity", pa.int64(), nullable=False),
])

workload_schema["pa_tasks_schema_optional"] = {
    "gpu_count": pa.field("gpu_count", pa.int32(), nullable=True),
    "gpu_capacity": pa.field("gpu_capacity", pa.float64(), nullable=True),
    "parents": pa.field("parents", pa.list_(pa.string()), nullable=True),
    "children": pa.field("children", pa.list_(pa.string()), nullable=True),
    "nature": pa.field("nature", pa.string(), nullable=True),
    "deadline": pa.field("deadline", pa.int64(), nullable=True)
}

workload_schema["tasks_columns_required"] = ["id", "submission_time", "duration", 
                              "cpu_count", "cpu_capacity", 
                              "mem_capacity"]

workload_schema["tasks_columns_optional"] = ["gpu_count", "gpu_capacity",
                             "parents", "children", "nature", "deadline"]

workload_schema["pa_fragments_schema_required"] = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("duration", pa.int64(), nullable=False),
    pa.field("cpu_count", pa.int32(), nullable=False),
    pa.field("cpu_usage", pa.float64(), nullable=False),
])

workload_schema["pa_fragments_schema_optional"] = {
    "gpu_count": pa.field("gpu_count", pa.int32(), nullable=True),
    "gpu_usage": pa.field("gpu_usage", pa.float64(), nullable=True)
}

workload_schema["fragments_columns_required"] = ["id", "duration", "cpu_count", "cpu_usage"]
workload_schema["fragments_columns_optional"] = ["gpu_count", "gpu_usage"]

workload_schema["tasks_name"] = "tasks"
workload_schema["fragments_name"] = "fragments"