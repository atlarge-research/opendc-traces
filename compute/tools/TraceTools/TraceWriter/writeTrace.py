# %%

import pandas as pd
from datetime import datetime 
import os

import pyarrow as pa
import pyarrow.parquet as pq
from TraceTools.TraceWriter.variables import meta_columns, fragments_columns, pa_schema_meta, pa_schema_fragments

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

