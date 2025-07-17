# %%

import pyarrow as pa
import pyarrow.parquet as pq

def writeTrace(df, pa_schema, output_path):
    pa_carbon_out = pa.Table.from_pandas(
        df = df,
        schema = pa_schema,
        preserve_index=False
    )
    
    pq.write_table(pa_carbon_out, output_path)
