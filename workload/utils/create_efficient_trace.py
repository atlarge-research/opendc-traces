# %%

import pandas as pd 

import sys 

sys.path.append("..")

from utils.write_workload_trace import writeTrace

# %%

def create_efficient_trace(df_tasks, df_fragments, workload_schema, output_path):
    if workload_schema not in ["v6_1", "v6_2"]:
        raise ValueError(f"Unsupported workload schema: {workload_schema}. Supported schemas are: v6_1, v6_2") 

    df_tasks = df_tasks.sort_values(by=["submission_time"])
    ids = list(df_tasks.id)
    df_fragments["id"] = pd.Categorical(df_fragments["id"], categories=ids, ordered=True)
    df_fragments.sort_values(by=["id"], inplace=True)

    # Remove empty fragments
    df_fragments = df_fragments[df_fragments["duration"] > 0]

    fragment_counts = df_fragments.groupby("id").size()
    df_tasks["num_fragments"] = df_tasks["id"].map(fragment_counts).fillna(0).astype(int)

    writeTrace(df_tasks, df_fragments, workload_schema, output_path)