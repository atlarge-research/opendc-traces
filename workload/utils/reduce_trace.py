# %%

import pandas as pd 

# %%

'''
Reduces the duration of the workload by removing all the task that 
start after the provided max_duration. 
Task that start before the max_duration but end after it will shortened to end at max_duration.
'''
def reduce_duration(df_tasks: pd.DataFrame, df_fragments: pd.DataFrame, max_duration: int):
    max_timestamp = df_tasks["submission_time"].min() + max_duration
    
    # Remove tasks that start after max_duration
    df_tasks = df_tasks[df_tasks["submission_time"] < max_timestamp]
    ids = list(df_tasks.id)
    df_fragments = df_fragments[df_fragments["id"].isin(ids)]

    df_tasks["duration"] = df_tasks.apply(lambda row: min(row["duration"], max_timestamp - row["submission_time"]), axis=1)

    # Remove fragments that start after the end of their task, and shorten the last fragment of each task
    # so the total duration of its fragments is equal to the duration of the task.
    task_duration = df_fragments["id"].map(df_tasks.set_index("id")["duration"])
    fragment_start = df_fragments.groupby("id")["duration"].cumsum() - df_fragments["duration"]

    df_fragments = df_fragments.assign(duration=df_fragments["duration"].clip(upper=task_duration - fragment_start))
    df_fragments = df_fragments[fragment_start < task_duration]

    return df_tasks, df_fragments    

'''
Reduce the number of tasks and fragments in a workload trace by retaining a specified ratio of tasks 
and their corresponding fragments.
'''
def reduce_tasks(df_tasks: pd.DataFrame, df_fragments: pd.DataFrame, retaining_ratio: float):
    task_ids = df_tasks["id"].sample(frac=retaining_ratio, random_state=42)
    df_tasks = df_tasks[df_tasks["id"].isin(task_ids)]
    df_fragments = df_fragments[df_fragments["id"].isin(task_ids)]

    return df_tasks, df_fragments