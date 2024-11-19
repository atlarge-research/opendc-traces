import numpy as np 
import pandas as pd 

from TraceTools.utils import addPTS, cleanFragments, getOverlappingFragmentsAfter, getOverlappingFragmentsBefore

def fixStartingTimeMeta(df_tasks, df_fragments):
    diff_start = (df_fragments.loc[df_fragments["isFirstRow"], "PTS_calc"].to_numpy() - df_tasks["start_time"].to_numpy())

    df_fragments.loc[df_fragments["isFirstRow"], "duration_td"] = df_fragments.loc[df_fragments["isFirstRow"], "duration_td"] + diff_start
    df_fragments.loc[df_fragments["isFirstRow"], "duration"] = df_fragments.loc[df_fragments["isFirstRow"], "duration_td"].dt.total_seconds() * 1000

    df_fragments.loc[df_fragments["isFirstRow"], "PTS_calc"] = df_tasks["start_time"].to_numpy()

    return df_tasks, df_fragments

def fixStartingTimeFragments(df_tasks, df_fragments):
    df_fragments = addPTS(df_tasks, df_fragments)
    task_ids = df_tasks["id"].unique()

    for task_id in task_ids:
        start_meta = df_tasks.loc[df_tasks["id"] == task_id, "start_time"].item()
        task_idx = df_fragments["id"] == task_id

        start_fragment = df_fragments.loc[task_idx, "PTS_calc"].min()
        start_diff = start_fragment - start_meta

        if start_diff == pd.Timedelta(0):
            continue

        df_tasks.loc[df_tasks["id"] == task_id, "start_time"] = start_fragment

    return df_tasks, df_fragments

def fixStartingTime(df_tasks, df_fragments, method="meta"):
    if method == "meta":
        return fixStartingTimeMeta(df_tasks, df_fragments)
    if method == "fragments":
        return fixStartingTimeFragments(df_tasks, df_fragments)