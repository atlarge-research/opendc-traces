import numpy as np 
import pandas as pd 

from TraceTools.utils import addPTS, cleanFragments, getOverlappingFragmentsAfter, getOverlappingFragmentsBefore

def fixStoppingTimeMeta(df_tasks, df_fragments):
    """Fix any missalligning end of tasks, by extending the final fragment to the end of the task

    Args:
        df_tasks (_type_): _description_
        df_fragments (_type_): _description_

    Returns:
        _type_: _description_
    """

    diff_stop = (df_fragments.loc[df_fragments["isLastRow"], "timestamp"].to_numpy() - df_tasks["stop_time"].to_numpy())

    df_fragments.loc[df_fragments["isLastRow"], "timestamp"] = df_tasks["stop_time"].to_numpy()
    df_fragments.loc[df_fragments["isLastRow"], "duration_td"] = df_fragments.loc[df_fragments["isLastRow"], "duration_td"] - diff_stop
    df_fragments.loc[df_fragments["isLastRow"], "duration"] = df_fragments.loc[df_fragments["isLastRow"], "duration_td"].dt.total_seconds() * 1000

    df_fragments = df_fragments[df_fragments["duration"] > 0]

    return df_tasks, df_fragments

def fixStoppingTimeFragments(df_tasks, df_fragments):
    """Fix any missalligning end of tasks, by changing the end of the task to the end of the final fragment

    Args:
        df_tasks (_type_): _description_
        df_fragments (_type_): _description_

    Returns:
        _type_: _description_
    """
    Warning("Not Implemented yet")
    return df_tasks, df_fragments


    df_fragments = addPTS(df_tasks, df_fragments)
    task_ids = df_tasks["id"].unique()

    for task_id in task_ids:
        start_task = df_tasks.loc[df_tasks["id"] == task_id, "start_time"].item()
        task_idx = df_fragments["id"] == task_id

        start_fragment = df_fragments.loc[task_idx, "PTS_calc"].min()
        start_diff = start_fragment - start_task

        if start_diff == pd.Timedelta(0):
            continue

        df_tasks.loc[df_tasks["id"] == task_id, "start_time"] = start_fragment

    return df_tasks, df_fragments

def fixStoppingTime(df_tasks, df_fragments, method="meta"):
    if method == "meta":
        return fixStoppingTimeMeta(df_tasks, df_fragments)
    if method == "fragments":
        return fixStoppingTimeFragments(df_tasks, df_fragments)