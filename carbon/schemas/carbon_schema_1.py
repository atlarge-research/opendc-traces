import pyarrow as pa

pa_schema_carbon = pa.schema([
    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
    pa.field("carbon_intensity", pa.float64(), nullable=False)
])