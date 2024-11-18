# %%

import pandas as pd
import numpy as np

# %%

df_tasks = pd.read_parquet("traces/surfsara/source/2022-10-01_2022-10-02/tasks.parquet")
df_fragments = pd.read_parquet("traces/surfsara/source/2022-10-01_2022-10-02/fragments.parquet")

# %%

df_fragments = df_fragments.sort_values(["id", "timestamp"])
df_tasks = df_tasks.sort_values("id")

# %%

task_ids = set(df_tasks.id.unique())
fragment_ids = set(df_fragments.id.unique())
task_overlap = task_ids and fragment_ids

df_tasks = df_tasks[df_tasks["id"].isin(task_overlap)]

df_fragments["duration_td"] = pd.to_timedelta(df_fragments["duration"], unit="ms")

# %%
df_fragments["PTS_calc"] = df_fragments["timestamp"] - df_fragments["duration_td"]

# %% Add True PTS

df_fragments["PTS_true"] = df_fragments["timestamp"].shift(1, fill_value=pd.Timestamp(0))
df_fragments["isFirstRow"] = df_fragments["id"].shift(1, fill_value=np.nan) != df_fragments["id"]
df_fragments.loc[df_fragments["isFirstRow"], "PTS_true"] = df_fragments.loc[df_fragments["isFirstRow"], "PTS_calc"]

# %% Add next PTS

df_fragments["PTS_next"] = df_fragments["PTS_calc"].shift(-1, fill_value=pd.Timestamp(0))
df_fragments["isLastRow"] = df_fragments["isFirstRow"].shift(-1, fill_value=True)
df_fragments.loc[df_fragments["isLastRow"], "PTS_next"] = df_fragments.loc[df_fragments["isLastRow"], "timestamp"]

# %% Fix start

diff_start = (df_fragments.loc[df_fragments["isFirstRow"], "PTS_calc"].to_numpy() - df_tasks["start_time"].to_numpy())

df_fragments.loc[df_fragments["isFirstRow"], "duration_td"] = df_fragments.loc[df_fragments["isFirstRow"], "duration_td"] + diff_start
df_fragments.loc[df_fragments["isFirstRow"], "duration"] = df_fragments.loc[df_fragments["isFirstRow"], "duration_td"].dt.total_seconds() * 1000

df_fragments.loc[df_fragments["isFirstRow"], "PTS_calc"] = df_tasks["start_time"].to_numpy()

# %% Fix stop


diff_stop = (df_fragments.loc[df_fragments["isLastRow"], "timestamp"].to_numpy() - df_tasks["stop_time"].to_numpy())

df_fragments.loc[df_fragments["isLastRow"], "timestamp"] = df_tasks["stop_time"].to_numpy()
df_fragments.loc[df_fragments["isLastRow"], "duration_td"] = df_fragments.loc[df_fragments["isLastRow"], "duration_td"] - diff_stop
df_fragments.loc[df_fragments["isLastRow"], "duration"] = df_fragments.loc[df_fragments["isLastRow"], "duration_td"].dt.total_seconds() * 1000

df_fragments = df_fragments[df_fragments["duration"] > 0]

# %%


# # %%


# # %%



# %%

task_id = df_tasks.id.unique()[0]
df_fragments[df_fragments["id"] == task_id].head()

# %%

df_tasks[df_tasks["id"] == task_id]

# %%

df_fragments[df_fragments["id"] == task_id].duration.sum()

# %%
(df_tasks[df_tasks["id"] == task_id].stop_time - df_tasks[df_tasks["id"] == task_id].start_time).dt.total_seconds()
