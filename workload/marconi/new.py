# %%

from query_tool import M100DataClient
import pandas as pd
import ast

def translateDF(df, metrics):
    """Translate the DataFrame turning each metric into a column

    Args:
        df (_type_): _description_
        metrics (_type_): _description_

    Returns:
        _type_: _description_
    """

    df_merged = df[df["metric"] == metrics[0]]

    df_merged = df_merged.rename({"value": metrics[0]}, axis=1)
    df_merged = df_merged.drop(["metric", "plugin", "year_month"], axis=1, errors="ignore")
    
    for metric in metrics[1:]:
        df_temp = df[df["metric"] == metric]
        if (len(df_temp) == 0):
            continue
        
        df_temp = df_temp.rename({"value": metric}, axis=1)
        df_temp = df_temp.drop(["metric", "plugin", "year_month"], axis=1, errors="ignore")

        if df_merged.empty:
            df_merged = df_temp
        else:
            df_merged = pd.merge(df_merged, df_temp, on=["timestamp", "node"])
    
    return df_merged

# %%

dataset_path = "data/22-09"

client = M100DataClient(dataset_path)

# %%

idle_power = 50
max_power = 350
num_cpu = 48
max_capacity = 2100 * num_cpu

ipmi_metrics = [ 'p1_power', 'p0_power']
fragment_columns = ["id", "duration", "cpu_count", "cpu_usage"]

start_day = "2022-09-01 00:00:00"
end_day = "2022-09-02 00:00:00"
# %%


df_jobs = client.query("job_info_marconi100")

# %%

df_jobs["duration"] = df_jobs.end_time - df_jobs.start_time
df_jobs = df_jobs[df_jobs["duration"] > pd.Timedelta(120, unit="s")]
df_jobs = df_jobs[df_jobs.nodes.notnull()]
df_jobs = df_jobs[df_jobs.nodes != "[]"]
df_jobs = df_jobs[df_jobs.num_nodes == 1]

def to_node(s):
    return str(ast.literal_eval(s)[0])

df_jobs["node"] = df_jobs.nodes.apply(to_node)

# %%

# CREAT TASKS.parquet
df_jobs["id"] = df_jobs.job_id.astype(str)
df_jobs["submission_time"] = df_jobs["start_time"]
df_jobs["cpu_count"] = num_cpu
df_jobs["cpu_capacity"] = max_capacity
df_jobs["mem_capacity"] = 100_000

df_tasks = df_jobs[["id", "submission_time", "duration", "cpu_count", "cpu_capacity", "mem_capacity"]].copy()

df_tasks.duration = (df_tasks.duration.dt.total_seconds() * 1000).astype(int)
df_tasks.submission_time = (df_tasks.submission_time.astype(int) / 10**6).astype(int)


df_tasks.to_parquet("results/tasks.parquet")

# %%

def get_node_fragments(node):
    df_fragments = pd.DataFrame([], columns=fragment_columns)

    # Load ipmi data related to a node
    df_ipmi = client.query(ipmi_metrics, node=str(node))
    df_ipmi_tr = translateDF(df_ipmi, df_ipmi.metric.unique())

    # Add cpu power and cpu count
    df_ipmi_tr["cpu_power"] = df_ipmi_tr["p0_power"] + df_ipmi_tr["p1_power"] 
    df_ipmi_tr["cpu_count"] = 48 * 2
    df_ipmi_tr["duration"] = 20_000

    dataframes = []

    for job_id, job_row in df_jobs[df_jobs.node == node].iterrows():
        job_idx = (df_ipmi_tr.timestamp >= job_row.start_time) & (df_ipmi_tr.timestamp <= job_row.end_time)

        df_fragments_job = df_ipmi_tr.loc[job_idx].copy()
        df_fragments_job = df_fragments_job.sort_values("timestamp")
        df_fragments_job["id"] = job_row.id.astype(str)

        # Add the cpu_usage
        df_fragments_job["cpu_util"] = (df_fragments_job["cpu_power"] - idle_power) / (max_power - idle_power)
        df_fragments_job["cpu_util"] = df_fragments_job["cpu_util"].clip(0,1)

        df_fragments_job["cpu_usage"] = df_fragments_job["cpu_util"] * max_capacity

        # Filter to only needed columns
        dataframes.append(df_fragments_job[fragment_columns])
        

        # df_fragments = df_fragments._append(df_fragments_job)

    

    return pd.concat(dataframes)

# %%

def convert_nodes(start_idx, end_idx, nodes):

    df_fragments = pd.DataFrame([], columns=fragment_columns)
    
    dataframes = []

    for i in range(start_idx, end_idx):

        dataframes.append(get_node_fragments(nodes[i]))

    df_fragments = pd.concat(dataframes)

    df_fragments.to_parquet(f"results/fragments_{start_idx}_{end_idx}.parquet")



# %%

all_nodes = list(df_jobs.node.unique())

step_size = 100
for start_idx in range(0, len(all_nodes), step_size):
    end_idx = min(start_idx+step_size, len(all_nodes))
    print(f"Converting nodes from index {start_idx} to {end_idx}")
    
    convert_nodes(start_idx, end_idx, all_nodes)

# %%


convert_nodes(0, 5, all_nodes)

# %%

df_0_5 = pd.read_parquet("results/fragments_0_5.parquet")

# %%


df_0_5.dtypes
