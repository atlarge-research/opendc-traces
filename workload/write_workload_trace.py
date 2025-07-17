# %%

import pyarrow as pa
import pyarrow.parquet as pq
import os


def writeTrace(df_tasks, df_fragments, workload_schema, output_path):
    if not os.path.exists(f"{output_path}"):
        os.makedirs(f"{output_path}") 

    schema_tasks = workload_schema["pa_tasks_schema_required"]
    tasks_columns = workload_schema["tasks_columns_required"]

    for column in workload_schema["tasks_columns_optional"]:
        if column in df_tasks.columns:
            schema_tasks = schema_tasks.append(workload_schema["pa_tasks_schema_optional"][column])
            tasks_columns.append(column)
        
    df_tasks = df_tasks[tasks_columns]
        
        
    schema_fragments = workload_schema["pa_fragments_schema_required"]
    fragments_columns = workload_schema["fragments_columns_required"]

    for column in workload_schema["fragments_columns_optional"]:
        if column in df_fragments.columns:
            schema_fragments = schema_fragments.append(workload_schema["pa_fragments_schema_optional"][column])
            fragments_columns.append(column)

    df_fragments = df_fragments[fragments_columns]

    pa_tasks_out = pa.Table.from_pandas(
        df = df_tasks,
        schema = schema_tasks,
        preserve_index=False
    )
    pa_fragments_out = pa.Table.from_pandas(
        df = df_fragments,
        schema = schema_fragments,
        preserve_index=False
    )
    
    pq.write_table(pa_tasks_out, f"{output_path}/tasks.parquet")
    pq.write_table(pa_fragments_out, f"{output_path}/fragments.parquet")

# %%
