import pyarrow as pa

pa_schema_failure = pa.schema([
    pa.field("failure_interval", pa.int64(), nullable=False),
    pa.field("failure_duration", pa.int64(), nullable=False),
    pa.field("failure_intensity", pa.float64(), nullable=False)
])