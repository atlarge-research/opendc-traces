import numpy as np
import pandas as pd


def checkStart(df_meta_task, df_fragments_task):
    return df_meta_task["start_time"] == df_fragments_task["PTS_calc"].min()

def checkStop(df_meta_task, df_fragments_task):
    return df_meta_task["stop_time"] == df_fragments_task["timestamp"].max()

def cleanFragments(df_fragments):
    return df_fragments[['id', 'timestamp', 'duration', 'cpu_count', 'cpu_usage']].sort_values(by=["id", "timestamp"], ascending=[True, True])

def cleanMeta(df_fragments):
    return df_meta[['id', 'start_time', 'stop_time', 'cpu_count', 'cpu_capacity', 'mem_capacity']].sort_values("start_time")

def addPTSCalc(df_fragments):
    df_fragments["duration_td"] = pd.to_timedelta(df_fragments["duration"], unit="ms")
    df_fragments["PTS_calc"] = df_fragments["timestamp"] - df_fragments["duration_td"]

    return df_fragments

def addPTSTrueTask(df_meta, df_fragments_task, task_id):
    df_meta_task = df_meta.loc[df_meta["id"] == task_id]
    timestamps = df_fragments_task["timestamp"].to_numpy()
    PTS_true = np.insert(timestamps[:-1], 0, df_meta_task["start_time"])

    return PTS_true

def addPTSTrue(df_meta, df_fragments):
    return df_fragments.groupby("id", group_keys=False) \
            .apply(lambda gdf: gdf.assign(PTS_true=lambda gdf_: addPTSTrueTask(df_meta, gdf, gdf.name)))

def addPTSCalcNext(df_meta, df_fragments, task_id):
    df_meta_task = df_meta[df_meta["id"] == task_id].iloc[0]

    PTS_calc = df_fragments["PTS_calc"].to_numpy()[1:]

    PTS_next = np.insert(PTS_calc, len(PTS_calc), df_fragments["timestamp"].max())

    return PTS_next


def addPTSNext(df_meta, df_fragments):
    return df_fragments.groupby("id", group_keys=False) \
        .apply(lambda gdf: gdf.assign(PTS_next=lambda gdf_: addPTSCalcNext(df_meta, gdf, gdf.name)))


def addPTS(df_meta, df_fragments):
    fragment_columns = df_fragments.columns

    if "PTS_calc" not in fragment_columns:
        df_fragments = addPTSCalc(df_fragments)
    
    if "PTS_true" not in fragment_columns:
        df_fragments = addPTSTrue(df_meta, df_fragments)

    if "PTS_next" not in fragment_columns:
        df_fragments = addPTSNext(df_meta, df_fragments)

    if "PTS_diff_before" not in fragment_columns:
        df_fragments["PTS_diff_before"] = df_fragments["PTS_next"] - df_fragments["timestamp"]

    if "PTS_diff_after" not in fragment_columns:
        df_fragments["PTS_diff_after"] = df_fragments["PTS_calc"] - df_fragments["PTS_true"]


    return df_fragments


def getCPUprevTask(df_meta, df_fragments_task, task_id):
    df_meta_task = df_meta[df_meta["id"] == task_id].iloc[0]

    cpu_usage = df_fragments_task["cpu_usage"].to_numpy()[:-1]

    cpu_usage = np.insert(cpu_usage, 0, cpu_usage[0])

    return cpu_usage

def addCPUprev(df_meta, df_fragments):
    return df_fragments.groupby("id", group_keys=False) \
            .apply(lambda gdf: gdf.assign(cpu_usage_prev=lambda gdf_: getCPUprevTask(df_meta, gdf, gdf.name)))



def getIncorrectFragments(df_meta, df_fragments):
    # Add the true previous sample time
    df_fragments = addPTS(df_meta, df_fragments)

    return df_fragments["PTS_diff_before"] != pd.Timedelta(0)

def getOverlappingFragmentsBefore(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)

    return df_fragments["PTS_diff_before"] < pd.Timedelta(0)

def getOverlappingFragmentsAfter(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)

    return df_fragments["PTS_diff_after"] < pd.Timedelta(0)

def getMissingFragmentsBefore(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)

    return df_fragments["PTS_diff_before"] > pd.Timedelta(0)

def getMissingFragmentsAfter(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)

    return df_fragments["PTS_diff_after"] > pd.Timedelta(0)


def validateTrace(df_meta, df_fragments):

    
    meta_tasks = set(df_meta["id"].unique())
    fragments_tasks = set(df_fragments["id"].unique())

    if (meta_tasks == fragments_tasks):
        print(f"The tasks in Meta and Fragments overlap fully")
    else: 
        error_message = f"not all tasks found in meta and fragments\n"
        error_message += f"Tasks in meta not in fragments: {meta_tasks - fragments_tasks}\n"
        error_message += f"Tasks in fragments not in meta: {fragments_tasks - meta_tasks}\n"

        raise ValueError(error_message)

    df_fragments = addPTS(df_meta, df_fragments)

    incorrect_idx = getIncorrectFragments(df_meta, df_fragments)
    missing_idx = getMissingFragmentsAfter(df_meta, df_fragments)
    overlapping_idx = getOverlappingFragmentsBefore(df_meta, df_fragments)


    task_count = len(meta_tasks)
    fragment_count = len(df_fragments)
    incorrect_count = incorrect_idx.sum()
    missing_count = missing_idx.sum()
    overlapping_count = overlapping_idx.sum()


    print(f"Trace overview")
    print(f"Total Tasks in trace:                 {task_count}")

    print(f"Total fragments in trace:             {fragment_count}")
    print(f"Number of incorrect fragments:        {incorrect_count}  ->  {incorrect_count / fragment_count * 100:.2f}%")
    print(f"Overlapping fragments in trace:       {overlapping_count}  ->  {overlapping_count / fragment_count * 100:.2f}%")
    print(f"Missing fragments in trace:           {missing_count}  ->  {missing_count / fragment_count * 100:.2f}%")
