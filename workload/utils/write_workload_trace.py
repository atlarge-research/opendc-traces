# %%

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import copy

from schemas.workload_schema_v1 import workload_schema as workload_schema_v1
from schemas.workload_schema_v2 import workload_schema as workload_schema_v2
from schemas.workload_schema_v3 import workload_schema as workload_schema_v3
from schemas.workload_schema_v3_1 import workload_schema as workload_schema_v3_1
from schemas.workload_schema_v4 import workload_schema as workload_schema_v4
from schemas.workload_schema_v5 import workload_schema as workload_schema_v5
from schemas.workload_schema_v5_1 import workload_schema as workload_schema_v5_1
from schemas.workload_schema_v6 import workload_schema as workload_schema_v6
from schemas.workload_schema_v6_1 import workload_schema as workload_schema_v6_1
from schemas.workload_schema_v6_2 import workload_schema as workload_schema_v6_2


'''
Convert the datetime and timedelta columns that the schema stores as integers to milliseconds.
pandas can store times in s, ms, us or ns (pandas 3 uses us by default), and pyarrow keeps that raw
integer when it converts a time to an integer. Without this, the unit in the trace would depend on how
the dataframe was made. All integer time columns in the schemas ("submission_time", "duration", "deadline")
are in milliseconds. Precision below a millisecond is truncated. Missing times (NaT) stay null.
'''
def _times_to_ms(df, schema) -> pd.DataFrame:
    columns = {}

    for field in schema:
        column = df[field.name]

        if not pa.types.is_integer(field.type):
            continue
        if not (pd.api.types.is_datetime64_any_dtype(column) or pd.api.types.is_timedelta64_dtype(column)):
            continue

        array = pa.array(column)
        ms_type = pa.duration("ms") if pa.types.is_duration(array.type) else pa.timestamp("ms", tz=array.type.tz)
        columns[field.name] = pd.arrays.ArrowExtensionArray(array.cast(ms_type, safe=False).cast(pa.int64()))

    return df.assign(**columns)

'''
Convert the tasks and fragments dataframes to pyarrow tables that follow workload_schema.
Only the required columns and the optional columns present in the dataframes are kept.
Datetime and timedelta columns are written as milliseconds when the schema stores them as integers.
'''
def toArrowTables(df_tasks, df_fragments, workload_schema):
    schema_tasks = copy.deepcopy(workload_schema["pa_tasks_schema_required"])
    tasks_columns = copy.deepcopy(workload_schema["pa_tasks_schema_required"].names)

    for column in workload_schema["pa_tasks_schema_optional"].keys():
        if column in df_tasks.columns:
            schema_tasks = schema_tasks.append(workload_schema["pa_tasks_schema_optional"][column])
            tasks_columns.append(column)
        
    df_tasks = df_tasks[tasks_columns]        
        
    schema_fragments = copy.deepcopy(workload_schema["pa_fragments_schema_required"])
    fragments_columns = copy.deepcopy(workload_schema["pa_fragments_schema_required"].names)

    for column in workload_schema["pa_fragments_schema_optional"].keys():
        if column in df_fragments.columns:
            schema_fragments = schema_fragments.append(workload_schema["pa_fragments_schema_optional"][column])
            fragments_columns.append(column)

    df_fragments = df_fragments[fragments_columns]

    df_tasks = _times_to_ms(df_tasks, schema_tasks)
    df_fragments = _times_to_ms(df_fragments, schema_fragments)

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

    return pa_tasks_out, pa_fragments_out

def _writeTrace(df_tasks, df_fragments, workload_schema, output_path):
    pa_tasks_out, pa_fragments_out = toArrowTables(df_tasks, df_fragments, workload_schema)

    if "trace" in workload_schema["tasks_name"]:
        if not os.path.exists(f"{output_path}/trace"):
            os.makedirs(f"{output_path}/trace", exist_ok=True)

    if not os.path.exists(f"{output_path}"):
        os.makedirs(f"{output_path}", exist_ok=True)

    pq.write_table(pa_tasks_out, f"{output_path}/{workload_schema['tasks_name']}.parquet")
    pq.write_table(pa_fragments_out, f"{output_path}/{workload_schema['fragments_name']}.parquet")


def writeTrace(df_tasks, df_fragments, version, output_path):
    if version == "v1":
        _writeTrace(df_tasks, df_fragments, workload_schema_v1, output_path)
        return
    if version == "v2":
        _writeTrace(df_tasks, df_fragments, workload_schema_v2, output_path)
        return
    if version == "v3":
        _writeTrace(df_tasks, df_fragments, workload_schema_v3, output_path)
        return
    if version == "v3_1":
        _writeTrace(df_tasks, df_fragments, workload_schema_v3_1, output_path)
        return
    if version == "v4":
        _writeTrace(df_tasks, df_fragments, workload_schema_v4, output_path)
        return
    if version == "v5":
        _writeTrace(df_tasks, df_fragments, workload_schema_v5, output_path)
        return
    if version == "v5_1":
        _writeTrace(df_tasks, df_fragments, workload_schema_v5_1, output_path)
        return
    if version == "v6":
        _writeTrace(df_tasks, df_fragments, workload_schema_v6, output_path)
        return
    if version == "v6_1":
        _writeTrace(df_tasks, df_fragments, workload_schema_v6_1, output_path)
        return
    if version == "v6_2":
        _writeTrace(df_tasks, df_fragments, workload_schema_v6_2, output_path)
        return

    raise ValueError(f"Unsupported version: {version}. Supported versions are: v1, v2, v3, v3_1, v4, v5, v5_1, v6, v6_1, v6_2")

# %%
