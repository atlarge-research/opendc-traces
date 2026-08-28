# %%

import pandas as pd 

import sys 

sys.path.append("..")

from workload.schemas.workload_schema_v5 import workload_schema
from utils.write_workload_trace import writeTrace

# %%

df_tasks_base = pd.read_parquet("traces/v5/borg_day/tasks.parquet")
df_fragments_base = pd.read_parquet("traces/v5/borg_day/fragments.parquet")
num_tasks = len(df_tasks_base)
copies = 7

# %%
# Correct id

id_to_index = {task_id: idx for idx, task_id in df_tasks_base["id"].items()}
df_tasks_base["id"] = df_tasks_base["id"].apply(lambda x: id_to_index[x])
df_fragments_base["id"] = df_fragments_base["id"].apply(lambda x: id_to_index[x])


# %%
# Copy 

for i in range(copies):

    df_tasks_copy = df_tasks_base.copy()
    df_fragments_copy = df_fragments_base.copy()

    # Correct ID

    df_tasks_copy["id"] = df_tasks_copy["id"].apply(lambda x: x + i*num_tasks)
    df_fragments_copy["id"] = df_fragments_copy["id"].apply(lambda x: x + i*num_tasks)

    print(f"Base id: {len(df_tasks_base)} tasks, {len(df_fragments_base)} fragments")
    print(f"Copy {i}: {len(df_tasks_copy)} tasks, {len(df_fragments_copy)} fragments")

    if (i == 0):
        df_tasks = df_tasks_copy
        df_fragments = df_fragments_copy

    else:
        df_tasks = pd.concat([df_tasks, df_tasks_copy], ignore_index=True)
        df_fragments = pd.concat([df_fragments, df_fragments_copy], ignore_index=True)


# %%

writeTrace(df_tasks, df_fragments, workload_schema, f"traces/v5/borg_day_x{copies}")
# %%
