import numpy as np 
import pandas as pd 

from utils import addPTS, addCPUprev, getMissingFragmentsAfter, \
                  getMissingFragmentsBefore, cleanFragments


def fixMissingFragmentsZero(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)

    missing_idx = getMissingFragmentsAfter(df_meta, df_fragments)

    new_fragments = df_fragments.loc[missing_idx].copy()
    new_fragments["timestamp"] = new_fragments["PTS_calc"]
    new_fragments["duration"] = new_fragments["PTS_diff_after"].dt.total_seconds()*1000
    new_fragments["cpu_usage"] = 0

    df_fragments_new = pd.concat([df_fragments, new_fragments])

    return cleanFragments(df_fragments_new)

def fixMissingFragmentsMean(df_meta, df_fragments):
    df_fragments = addPTS(df_meta, df_fragments)
    df_fragments = addCPUprev(df_meta, df_fragments)

    missing_before_idx = getMissingFragmentsBefore(df_meta, df_fragments)
    missing_after_idx = getMissingFragmentsAfter(df_meta, df_fragments)

    new_fragments = df_fragments.loc[missing_after_idx].copy()
    new_fragments["timestamp"] = new_fragments["PTS_calc"]
    new_fragments["duration"] = new_fragments["PTS_diff_after"].dt.total_seconds()*1000
    new_fragments["cpu_usage"] = (new_fragments["cpu_usage"] + new_fragments["cpu_usage_prev"]) / 2

    df_fragments_new = pd.concat([df_fragments, new_fragments])

    return cleanFragments(df_fragments_new)

def fixMissingFragmentsBefore(df_meta, df_fragments):

    df_fragments = addPTS(df_meta, df_fragments)

    missing_idx = getMissingFragmentsBefore(df_meta, df_fragments)

    df_fragments.loc[missing_idx, "duration"] = df_fragments.loc[missing_idx, "duration"] + df_fragments.loc[missing_idx, "PTS_diff_before"].dt.total_seconds()*1000
    df_fragments.loc[missing_idx, "timestamp"] = df_fragments.loc[missing_idx, "timestamp"] + df_fragments.loc[missing_idx, "PTS_diff_before"]

    return cleanFragments(df_fragments)

def fixMissingFragmentsAfter(df_meta, df_fragments):

    df_fragments = addPTS(df_meta, df_fragments)

    missing_idx = getMissingFragmentsAfter(df_meta, df_fragments)

    print(missing_idx)

    df_fragments.loc[missing_idx, "duration"] = df_fragments.loc[missing_idx, "duration"] + df_fragments.loc[missing_idx, "PTS_diff_after"].dt.total_seconds()*1000

    return cleanFragments(df_fragments)


def fixMissingFragments(df_meta, df_fragments, method="zero"):
    if method == "zero":
        return fixMissingFragmentsZero(df_meta, df_fragments)
    if method == "mean":
        return fixMissingFragmentsMean(df_meta, df_fragments)
    if method == "before":
        return fixMissingFragmentsBefore(df_meta, df_fragments)
    if method == "after":
        return fixMissingFragmentsAfter(df_meta, df_fragments)

    else:
        raise ValueError(f"Method: {method} is unknown. Please pick a method from [zero, mean, after, before]")
