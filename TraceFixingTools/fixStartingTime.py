import numpy as np 
import pandas as pd 

def fixStartingTimeMeta(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)
    task_ids = df_meta["id"].unique()

    for task_id in task_ids:
        start_meta = df_meta.loc[df_meta["id"] == task_id, "start_time"].item()
        task_idx = df_fragments["id"] == task_id

        start_fragment = df_fragments.loc[task_idx, "PTS_calc"].min()
        start_diff = start_fragment - start_meta

        if start_diff == pd.Timedelta(0):
            continue

        start_fragment_idx = (task_idx) & (df_fragments["PTS_calc"] == start_fragment)
        df_fragments.loc[start_fragment_idx, "duration"] = df_fragments.loc[start_fragment_idx, "duration"] + start_diff.total_seconds()*1000

    return df_meta, df_fragments

def fixStartingTimeFragments(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)
    task_ids = df_meta["id"].unique()

    for task_id in task_ids:
        start_meta = df_meta.loc[df_meta["id"] == task_id, "start_time"].item()
        task_idx = df_fragments["id"] == task_id

        start_fragment = df_fragments.loc[task_idx, "PTS_calc"].min()
        start_diff = start_fragment - start_meta

        if start_diff == pd.Timedelta(0):
            continue

        df_meta.loc[df_meta["id"] == task_id, "start_time"] = start_fragment

    return df_meta, df_fragments

def fixStartingTime(df_meta, df_fragments, method="meta"):
    if method == "meta":
        return fixStartingTimeMeta(df_meta, df_fragments)
    if method == "fragments":
        return fixStartingTimeFragments(df_meta, df_fragments)