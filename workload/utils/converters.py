# %%

## A collection of functions to convert workload traces between different schema versions.

import pandas as pd
from schemas.workload_schema_v1 import workload_schema as workload_schema_v1
from schemas.workload_schema_v2 import workload_schema as workload_schema_v2
from schemas.workload_schema_v3 import workload_schema as workload_schema_v3
from schemas.workload_schema_v4 import workload_schema as workload_schema_v4
from schemas.workload_schema_v5 import workload_schema as workload_schema_v5

from utils.write_workload_trace import writeTrace

def convert_v1_to_v2(trace_name):
    df_tasks = pd.read_parquet(f"traces/v1/{trace_name}/trace/meta.parquet")
    df_fragments = pd.read_parquet(f"traces/v1/{trace_name}/trace/trace.parquet")

    df_tasks = df_tasks.rename(columns={"start_time": "submission_time"})

    writeTrace(df_tasks, df_fragments, workload_schema_v2, f"traces/v2/{trace_name}")

def convert_v2_to_v3(trace_name):
    df_tasks = pd.read_parquet(f"traces/v2/{trace_name}/tasks.parquet")
    df_fragments = pd.read_parquet(f"traces/v2/{trace_name}/fragments.parquet")

    task_durations = df_fragments.groupby("id").duration.sum()

    df_tasks = df_tasks.merge(task_durations, on="id", how="left", suffixes=("", "_from_fragments"))

    df_tasks = df_tasks.dropna(axis=0, subset=["duration"])
    
    writeTrace(df_tasks, df_fragments, workload_schema_v3, f"traces/v3/{trace_name}")

def convert_v3_to_v4(trace_name):
    df_tasks = pd.read_parquet(f"traces/v3/{trace_name}/tasks.parquet")
    df_fragments = pd.read_parquet(f"traces/v3/{trace_name}/fragments.parquet")
    
    df_tasks.id = df_tasks.id.astype("int32")
    df_fragments.id = df_fragments.id.astype("int32")

    writeTrace(df_tasks, df_fragments, workload_schema_v4, f"traces/v4/{trace_name}")
    
def convert_v4_to_v5(trace_name):
    df_tasks = pd.read_parquet(f"traces/v4/{trace_name}/tasks.parquet")
    df_fragments = pd.read_parquet(f"traces/v4/{trace_name}/fragments.parquet")
    
    if ("nature" in df_tasks.columns):
        df_tasks["deferrable"] = df_tasks.nature == "deferrable"

    writeTrace(df_tasks, df_fragments, workload_schema_v5, f"traces/v5/{trace_name}")

def convert_v5_to_v4(trace_name):
    df_tasks = pd.read_parquet(f"traces/v5/{trace_name}/tasks.parquet")
    df_fragments = pd.read_parquet(f"traces/v5/{trace_name}/fragments.parquet")

    if ("deferrable" in df_tasks.columns):
        df_tasks["nature"] = df_tasks.deferrable.map({True: "deferrable", False: "not_deferrable"})

    writeTrace(df_tasks, df_fragments, workload_schema_v5, f"traces/v5/{trace_name}")


def convert_v4_to_v3(trace_name):
    df_tasks = pd.read_parquet(f"traces/v4/{trace_name}/tasks.parquet")
    df_fragments = pd.read_parquet(f"traces/v4/{trace_name}/fragments.parquet")

    df_tasks.id = df_tasks.id.astype(str)
    df_fragments.id = df_fragments.id.astype(str)

    df_fragments = df_fragments.merge(df_tasks[["id", "cpu_count"]], on="id", how="left")

    writeTrace(df_tasks, df_fragments, workload_schema_v3, f"traces/v3/{trace_name}")

def convert_v3_to_v2(trace_name):
    df_tasks = pd.read_parquet(f"traces/v3/{trace_name}/tasks.parquet")
    df_fragments = pd.read_parquet(f"traces/v3/{trace_name}/fragments.parquet")

    writeTrace(df_tasks, df_fragments, workload_schema_v2, f"traces/v2/{trace_name}")

def convert_v2_to_v1(trace_name):
    df_tasks = pd.read_parquet(f"traces/v2/{trace_name}/tasks.parquet")
    df_fragments = pd.read_parquet(f"traces/v2/{trace_name}/fragments.parquet")

    task_durations = df_fragments.groupby("id").duration.sum()
    task_durations = pd.to_timedelta(task_durations, unit="ms")

    first_fragment_durations = df_fragments.groupby("id").first().duration
    first_fragment_durations = pd.to_timedelta(first_fragment_durations, unit="ms")

    df_tasks = df_tasks.merge(task_durations, on="id", how="left")
    df_tasks = df_tasks.merge(first_fragment_durations, on="id", how="left", suffixes=("", "_first"))

    df_tasks["start_time"] = df_tasks["submission_time"]
    df_tasks["stop_time"] = df_tasks["submission_time"] + df_tasks["duration"] + df_tasks["duration_first"]

    df_tasks = df_tasks.dropna(axis=0, subset=["stop_time"])

    df_fragments = df_fragments.merge(df_tasks[["id", "start_time"]], on="id", how="left")

    df_fragments["duration_dt"] = pd.to_timedelta(df_fragments["duration"], unit="ms")

    df_fragments["cumulative_duration_dt"] = df_fragments.groupby("id")["duration_dt"].cumsum()

    df_fragments["timestamp"] = df_fragments["start_time"] + df_fragments["cumulative_duration_dt"]

    df_fragments[["id", "timestamp"]]

    writeTrace(df_tasks, df_fragments, workload_schema_v1, f"traces/v1/{trace_name}")
