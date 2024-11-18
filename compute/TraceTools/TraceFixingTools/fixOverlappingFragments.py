import numpy as np 
import pandas as pd 

from TraceTools.utils import addPTS,  getOverlappingFragmentsAfter, getOverlappingFragmentsBefore

def fixOverlappingFragmentsBefore(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)
    overlap_idx = getOverlappingFragmentsAfter(df_meta, df_fragments)

    df_fragments.loc[overlap_idx, "duration"] = df_fragments.loc[overlap_idx, "duration"] + df_fragments.loc[overlap_idx, "PTS_diff_after"].dt.total_seconds() * 1000

    return df_fragments

def fixOverlappingFragmentsAfter(df_meta, df_fragments):
    # df_fragments = addPTS(df_meta, df_fragments)
    print(df_fragments.columns)
    overlap_idx = getOverlappingFragmentsBefore(df_meta, df_fragments)

    df_fragments.loc[overlap_idx, "timestamp"] = df_fragments.loc[overlap_idx, "timestamp"] + df_fragments.loc[overlap_idx, "PTS_diff_before"]
    df_fragments.loc[overlap_idx, "duration"] = df_fragments.loc[overlap_idx, "duration"] + df_fragments.loc[overlap_idx, "PTS_diff_before"].dt.total_seconds() * 1000

    return df_fragments

def fixOverlappingFragmentsMean(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)
    overlap_before_idx = getOverlappingFragmentsBefore(df_meta, df_fragments)
    overlap_after_idx = getOverlappingFragmentsAfter(df_meta, df_fragments)

    new_fragments = df_fragments[overlap_before_idx].copy()

    new_fragments["duration"] = -new_fragments["PTS_diff_before"].dt.total_seconds()*1000
    new_fragments["cpu_usage"] = (df_fragments.loc[overlap_before_idx, "cpu_usage"].to_numpy() + df_fragments.loc[overlap_after_idx, "cpu_usage"].to_numpy()) / 2

    df_fragments.loc[overlap_after_idx, "duration"] = df_fragments.loc[overlap_after_idx, "duration"] + df_fragments.loc[overlap_after_idx, "PTS_diff_after"].dt.total_seconds() * 1000

    df_fragments.loc[overlap_before_idx, "timestamp"] = df_fragments.loc[overlap_before_idx, "timestamp"] + df_fragments.loc[overlap_before_idx, "PTS_diff_before"]
    df_fragments.loc[overlap_before_idx, "duration"] = df_fragments.loc[overlap_before_idx, "duration"] + df_fragments.loc[overlap_before_idx, "PTS_diff_before"].dt.total_seconds() * 1000

    df_fragments_new = pd.concat([df_fragments, new_fragments])

    return df_fragments_new

def fixOverlappingFragments(df_meta, df_fragments, method="after"):
    if method == "before":
        return fixOverlappingFragmentsBefore(df_meta, df_fragments)
    if method == "after":
        return fixOverlappingFragmentsAfter(df_meta, df_fragments)
    if method == "mean":
        return fixOverlappingFragmentsMean(df_meta, df_fragments)

    else:
        raise ValueError(f"method: {method} is unknown. Please pick a method from [after, before, mean]")
