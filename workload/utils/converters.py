# %%

## A collection of functions to convert workload traces between different schema versions.
## Converters never modify the dataframes passed to them, they always return new dataframes.
## Converters give a DataLossWarning when they drop columns that contain data.
## Silence them with: warnings.simplefilter("ignore", DataLossWarning)

import warnings

import numpy as np
import pandas as pd
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

from utils.write_workload_trace import writeTrace

#################################################################################################
## Helpers
#################################################################################################

'''
Traces written by OpenDC v2.0 store times as epoch milliseconds instead of timestamps.
'''
def _to_datetime(column) -> pd.Series:
    if pd.api.types.is_numeric_dtype(column):
        return pd.to_datetime(column, unit="ms")
    return column

def _to_epoch_ms(column) -> pd.Series:
    return column.dt.as_unit("ms").astype("int64")

'''
Replace every task id in a "parents" or "children" list using map_id.
Tasks without dependencies (None) are left as they are.
'''
def _map_ids(ids, map_id):
    if isinstance(ids, (list, tuple, np.ndarray)):
        return [map_id(i) for i in ids]
    return ids

def _map_dependencies(df_tasks, map_id) -> pd.DataFrame:
    for column in ["parents", "children"]:
        if column in df_tasks.columns:
            df_tasks[column] = df_tasks[column].map(lambda ids: _map_ids(ids, map_id))
    return df_tasks

class DataLossWarning(UserWarning):
    pass

'''
Drop columns that are not supported in to_version.
Gives a DataLossWarning for every dropped column that contains data, because that data is lost.
Columns that are missing or only contain nulls are dropped without a warning.
'''
def _drop_unsupported(df, columns, table, to_version) -> pd.DataFrame:
    lost = [column for column in columns if column in df.columns and df[column].notna().any()]

    if lost:
        warnings.warn(
            f"Dropping {table} column(s) {', '.join(lost)}, because they are not supported in {to_version}. Their data is lost.",
            DataLossWarning,
            stacklevel=3,
        )

    return df.drop(columns=columns, errors="ignore")

#################################################################################################
## Upgrade
#################################################################################################

'''
Convert a workload trace from schema v1 to v2.
Task:
    Renamed columns to match the new schema.
    Added "cpu_capacity", which v1 does not have. It is set to the highest cpu usage of the task,
    so no usage is capped.
    "stop_time" is "endTime" plus the duration of the last fragment of the task (see note).
    "mem_capacity" is "requiredMemory" * 1000 (see note).
Fragment:
    Renamed columns to match the new schema. Removed flops column.
    "timestamp" is "time" plus the duration of the fragment (see note).

Note: v1 and v2 use a different point in time for fragments.
    v1: "time" is the start of a fragment, and "endTime" is the start of the last fragment of a task.
    v2: "timestamp" is the end of a fragment, and "stop_time" is the end of the last fragment of a task.
    This was checked on the same 50 Bitbrains tasks written by OpenDC in both versions: for every task,
    the first fragment has time == submissionTime in v1 and timestamp == start_time + duration in v2.

Note: In those same traces, "mem_capacity" in v2 is about 1000 times "requiredMemory" in v1
    (requiredMemory == mem_capacity // 1000 for 45 of the 50 tasks). From v2 until v6_2,
    memory was stored in KiB, which was a bug in the traces that is fixed in v6_2.
'''
def convert_v1_to_v2(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = df_tasks.rename(
        columns={
            "submissionTime": "start_time",
            "endTime": "stop_time",
            "maxCores": "cpu_count",
            "requiredMemory": "mem_capacity",
        }
    )

    last_duration = df_fragments.sort_values("time", kind="stable").groupby("id")["duration"].last()

    df_tasks["start_time"] = _to_datetime(df_tasks["start_time"])
    df_tasks["stop_time"] = _to_datetime(df_tasks["stop_time"]) + pd.to_timedelta(df_tasks["id"].map(last_duration).fillna(0), unit="ms")
    df_tasks["mem_capacity"] = df_tasks["mem_capacity"] * 1000

    max_cpu_usage = df_fragments.groupby("id")["cpuUsage"].max()
    df_tasks["cpu_capacity"] = df_tasks["id"].map(max_cpu_usage).fillna(0.0)

    df_fragments = df_fragments.rename(
        columns={
            "time": "timestamp",
            "cores": "cpu_count",
            "cpuUsage": "cpu_usage",
        }
    )
    df_fragments = _drop_unsupported(df_fragments, ["flops"], "fragments", "v2")

    df_fragments["timestamp"] = _to_datetime(df_fragments["timestamp"]) + pd.to_timedelta(df_fragments["duration"], unit="ms")

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v2 to v3.
The only difference between v2 and v3 is the output file format.
'''
def convert_v2_to_v3(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    return df_tasks, df_fragments

'''
Convert a workload trace from schema v3 to v3.1.
Task:
    Renamed "start_time" to "submission_time" and changed it from a timestamp to epoch milliseconds.
    Replaced "stop_time" with "duration" (in milliseconds) to indicate the duration of a task.
Fragment: No changes.
'''
def convert_v3_to_v3_1(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = df_tasks.rename(
        columns={
            "start_time": "submission_time",
        }
    )

    df_tasks["duration"] = (df_tasks["stop_time"] - df_tasks["submission_time"]) // pd.Timedelta(milliseconds=1)
    df_tasks["submission_time"] = _to_epoch_ms(df_tasks["submission_time"])

    df_tasks = df_tasks.drop(columns=["stop_time"])

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v3.1 to v4.
All new columns introduced in v4 are optional, so no changes are needed.
'''
def convert_v3_1_to_v4(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    return df_tasks, df_fragments

'''
Convert a workload trace from schema v4 to v5.
All new columns introduced in v5 are optional, so no changes are needed.
'''
def convert_v4_to_v5(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    return df_tasks, df_fragments

'''
Convert a workload trace from schema v5 to v5_1.
The id column type has changed from string to int32,
and a new optional column "name" has been added to the tasks table.
The old string ids are stored in "name" when keep_name is True.
If they are not kept and differ from the new ids, a warning is given.
"parents" and "children" are updated to the new ids.
"timestamp", "cpu_count" and "gpu_count" have been removed from the fragments table.
'''
def convert_v5_to_v5_1(df_tasks, df_fragments, keep_name=False) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = df_tasks.rename(
        columns={
            "id": "name",
        }
    )

    df_tasks["id"] = np.arange(len(df_tasks), dtype="int32")

    id_map = pd.Series(df_tasks["id"].values, index=df_tasks["name"])

    df_tasks = _map_dependencies(df_tasks, id_map.__getitem__)

    if not keep_name:
        if (df_tasks["name"].astype(str) != df_tasks["id"].astype(str)).any():
            warnings.warn(
                "Replacing the task ids with integer ids. The original ids are lost, use keep_name=True to store them in the \"name\" column.",
                DataLossWarning,
                stacklevel=2,
            )
        df_tasks = df_tasks.drop(columns=["name"])

    df_fragments = _drop_unsupported(df_fragments, ["timestamp", "cpu_count", "gpu_count"], "fragments", "v5_1")
    df_fragments["id"] = df_fragments["id"].map(id_map).astype("int32")

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v5_1 to v6.
"nature" column has been replaced with "deferrable" column in the tasks table.
'''
def convert_v5_1_to_v6(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "nature" in df_tasks.columns:
        df_tasks = df_tasks.assign(deferrable=df_tasks["nature"] == "deferrable").drop(columns=["nature"])

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v6 to v6_1.
All new columns introduced in v6_1 are optional, so no changes are needed.
'''
def convert_v6_to_v6_1(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    return df_tasks, df_fragments

'''
Convert a workload trace from schema v6_1 to v6_2.
The schema is the same, but OpenDC now reads "mem_capacity" as MiB instead of KiB.
Values are rounded down, so converting back to v6_1 can give a slightly lower value.
'''
def convert_v6_1_to_v6_2(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = df_tasks.assign(mem_capacity=df_tasks["mem_capacity"] // 1024)

    return df_tasks, df_fragments

#################################################################################################
## Downgrade
#################################################################################################

'''
Convert a workload trace from schema v6_2 to v6_1.
The schema is the same, but OpenDC v6_1 reads "mem_capacity" as KiB instead of MiB.
'''
def convert_v6_2_to_v6_1(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = df_tasks.assign(mem_capacity=df_tasks["mem_capacity"] * 1024)

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v6_1 to v6.
Removed the "num_fragments" column from the tasks table.
'''
def convert_v6_1_to_v6(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = _drop_unsupported(df_tasks, ["num_fragments"], "tasks", "v6")

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v6 to v5_1.
"deferrable" column has been replaced with "nature" column in the tasks table.
'''
def convert_v6_to_v5_1(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "deferrable" in df_tasks.columns:
        df_tasks = df_tasks.assign(nature=df_tasks["deferrable"].map({True: "deferrable", False: None})).drop(columns=["deferrable"])

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v5_1 to v5.
Converted id back into string, using "name" when it is available, and removed the "name" column.
"parents" and "children" are updated to the new ids.
Added "timestamp", "cpu_count" and "gpu_count" back to the fragments table.
'''
def convert_v5_1_to_v5(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_fragments = df_fragments.drop(columns=["timestamp", "cpu_count", "gpu_count"], errors="ignore")

    if "name" in df_tasks.columns:
        new_ids = df_tasks["name"].fillna(df_tasks["id"].astype(str))
        id_map = pd.Series(new_ids.values, index=df_tasks["id"])

        df_tasks = df_tasks.drop(columns=["name"])
        df_tasks = _map_dependencies(df_tasks, id_map.__getitem__)
        df_tasks["id"] = new_ids.values
        df_fragments["id"] = df_fragments["id"].map(id_map)
    else:
        # Without "name", the new ids are just the old ids as strings, so no lookup in the tasks table is needed.
        df_tasks = df_tasks.assign(id=df_tasks["id"].astype(str))
        df_tasks = _map_dependencies(df_tasks, str)
        df_fragments["id"] = df_fragments["id"].astype(str)

    if "gpu_count" in df_tasks.columns:
        df_tasks["gpu_count"] = df_tasks["gpu_count"].fillna(0).astype("int32")

    # Putting "cpu_count" and "gpu_count" back into the fragments table, as they were in v5.
    task_columns = [column for column in ["id", "submission_time", "cpu_count", "gpu_count"] if column in df_tasks.columns]
    df_fragments = df_fragments.merge(df_tasks[task_columns], on="id", how="left")

    # Adding "timestamp" column to the fragments table, as it was in v5.
    # The timestamp of a fragment is its end time: the task's submission time plus the duration of all fragments up to and including it.
    cumulative_duration = df_fragments.groupby("id")["duration"].cumsum()
    df_fragments["timestamp"] = pd.to_datetime(df_fragments["submission_time"], unit="ms") + pd.to_timedelta(cumulative_duration, unit="ms")

    df_fragments = df_fragments.drop(columns=["submission_time"])

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v5 to v4.
Removed the columns introduced in v5.
'''
def convert_v5_to_v4(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = _drop_unsupported(df_tasks, ["parents", "children", "gpu_count", "gpu_capacity"], "tasks", "v4")
    df_fragments = _drop_unsupported(df_fragments, ["gpu_count", "gpu_usage"], "fragments", "v4")

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v4 to v3_1.
Removed the columns introduced in v4.
'''
def convert_v4_to_v3_1(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = _drop_unsupported(df_tasks, ["nature", "deadline"], "tasks", "v3_1")

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v3_1 to v3.
Task:
    Renamed "submission_time" to "start_time" and changed it from epoch milliseconds to a timestamp.
    Replaced "duration" with "stop_time".
Fragment: No changes.
'''
def convert_v3_1_to_v3(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = df_tasks.rename(
        columns={
            "submission_time": "start_time",
        }
    )

    df_tasks["start_time"] = pd.to_datetime(df_tasks["start_time"], unit="ms")
    df_tasks["stop_time"] = df_tasks["start_time"] + pd.to_timedelta(df_tasks["duration"], unit="ms")

    df_tasks = df_tasks.drop(columns=["duration"])

    return df_tasks, df_fragments

'''
Convert a workload trace from schema v3 to v2.
v3 only changed the output file format, so no changes are needed.
'''
def convert_v3_to_v2(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    return df_tasks, df_fragments

'''
Convert a workload trace from schema v2 to v1.
Task:
    Renamed columns to match the old schema. Removed "cpu_capacity".
    "endTime" is "stop_time" minus the duration of the last fragment of the task.
    "requiredMemory" is "mem_capacity" // 1000.
Fragment:
    Renamed columns to match the old schema.
    "time" is "timestamp" minus the duration of the fragment.
    Added "flops", computed the same way as in traces written by OpenDC v2.0: cpu usage (MHz) * duration (ms) / 1000.
See the notes on convert_v1_to_v2 for why the times and memory change.
'''
def convert_v2_to_v1(df_tasks, df_fragments) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_tasks = df_tasks.rename(
        columns={
            "start_time": "submissionTime",
            "stop_time": "endTime",
            "cpu_count": "maxCores",
            "mem_capacity": "requiredMemory",
        }
    )
    df_tasks = _drop_unsupported(df_tasks, ["cpu_capacity"], "tasks", "v1")

    last_duration = df_fragments.sort_values("timestamp", kind="stable").groupby("id")["duration"].last()
    df_tasks["endTime"] = df_tasks["endTime"] - pd.to_timedelta(df_tasks["id"].map(last_duration).fillna(0), unit="ms")
    df_tasks["requiredMemory"] = df_tasks["requiredMemory"] // 1000

    df_fragments = df_fragments.rename(
        columns={
            "timestamp": "time",
            "cpu_count": "cores",
            "cpu_usage": "cpuUsage",
        }
    )

    df_fragments["time"] = df_fragments["time"] - pd.to_timedelta(df_fragments["duration"], unit="ms")
    df_fragments["flops"] = (df_fragments["cpuUsage"] * df_fragments["duration"] / 1000).astype("int64")

    return df_tasks, df_fragments


#################################################################################################
## Chaining
#################################################################################################

VERSIONS = ["v1", "v2", "v3", "v3_1", "v4", "v5", "v5_1", "v6", "v6_1", "v6_2"]

# UPGRADERS[i] converts a trace from VERSIONS[i] to VERSIONS[i + 1].
UPGRADERS = [
    convert_v1_to_v2,
    convert_v2_to_v3,
    convert_v3_to_v3_1,
    convert_v3_1_to_v4,
    convert_v4_to_v5,
    convert_v5_to_v5_1,
    convert_v5_1_to_v6,
    convert_v6_to_v6_1,
    convert_v6_1_to_v6_2,
]

# DOWNGRADERS[i] converts a trace from VERSIONS[i + 1] to VERSIONS[i].
DOWNGRADERS = [
    convert_v2_to_v1,
    convert_v3_to_v2,
    convert_v3_1_to_v3,
    convert_v4_to_v3_1,
    convert_v5_to_v4,
    convert_v5_1_to_v5,
    convert_v6_to_v5_1,
    convert_v6_1_to_v6,
    convert_v6_2_to_v6_1,
]

'''
Return the list of converters needed to go from from_version to to_version, in order.
Works in both directions.
'''
def get_converters(from_version, to_version) -> list:
    start = VERSIONS.index(from_version)
    end = VERSIONS.index(to_version)

    if start <= end:
        return UPGRADERS[start:end]

    return DOWNGRADERS[end:start][::-1]

'''
Convert a workload trace from from_version to to_version by chaining the converters.
'''
def convert(df_tasks, df_fragments, from_version, to_version) -> tuple[pd.DataFrame, pd.DataFrame]:
    for converter in get_converters(from_version, to_version):
        df_tasks, df_fragments = converter(df_tasks, df_fragments)

    return df_tasks, df_fragments

# %%
