import pandas as pd 
from TraceTools.TraceFixingTools.fixMissingFragments import fixMissingFragments
from TraceTools.TraceFixingTools.fixOverlappingFragments import fixOverlappingFragments
from TraceTools.TraceFixingTools.fixStartingTime import fixStartingTime
from TraceTools.TraceFixingTools.fixStoppingTime import fixStoppingTime

from TraceTools.utils import addPTS

def fixTrace(df_tasks, df_fragments, start_fix="meta", stop_fix="meta", 
                                    missing_fix="before", overlap_fix="after"):

    task_ids = set(df_tasks.id.unique())
    fragment_ids = set(df_fragments.id.unique())
    task_overlap = task_ids and fragment_ids

    df_tasks = df_tasks[df_tasks["id"].isin(task_overlap)]

    print("Sorting dfs")
    df_fragments = df_fragments.sort_values(["id", "timestamp"])
    df_tasks = df_tasks.sort_values("id")


    print("adding PTS")
    df_fragments = addPTS(df_tasks, df_fragments)

    print(df_fragments.columns)


    print("fixing starting time")
    df_tasks, df_fragments = fixStartingTime(df_tasks, df_fragments, method=start_fix)

    print("fixing stop time")
    df_tasks, df_fragments = fixStoppingTime(df_tasks, df_fragments, method=stop_fix)

    print("fixing missing fragments time")
    df_fragments = fixMissingFragments(df_tasks, df_fragments, method=missing_fix)

    print("fixing overlapping fragments")
    df_fragments = fixOverlappingFragments(df_tasks, df_fragments, method=overlap_fix)


    task_ids = set(df_tasks.id.unique())
    fragment_ids = set(df_fragments.id.unique())
    task_overlap = task_ids and fragment_ids

    df_tasks = df_tasks[df_tasks["id"].isin(task_overlap)]

    return df_tasks, df_fragments