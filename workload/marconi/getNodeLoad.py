# %% 

from query_tool import M100DataClient
import pandas as pd
import ast
import numpy as np

import matplotlib.pyplot as plt

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

def rollavg(a,n):
    # see https://stackoverflow.com/questions/42101082/fast-numpy-roll
    'Numpy array rolling, edge handling'
    assert n%2==1
    a = np.pad(a,(0,n-1-n//2), 'constant')*np.ones(n)[:,None]
    m = a.shape[1]
    idx = np.mod((m-1)*np.arange(n)[:,None] + np.arange(m), m) # Rolling index
    out = a[np.arange(-n//2,n//2)[:,None], idx]
    d = np.hstack((np.arange(1,n),np.ones(m-2*n+1+n//2)*n,np.arange(n,n//2,-1)))
    return (out.sum(axis=0)/d)[n//2:]


def plotTwo(dataA_x, dataA_y, labelA, dataB_x, dataB_y, labelB):

    fig, ax1 = plt.subplots()

    color = 'tab:green'
    ax1.set_xlabel("Timestamp (s)")
    ax1.set_ylabel(labelA, color=color)
    ax1.plot(dataA_x, dataA_y, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second Axes that shares the same x-axis

    color = 'tab:red'
    ax2.set_ylabel(labelB, color=color)  # we already handled the x-label with ax1
    ax2.plot(dataB_x, dataB_y, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()




dataset_path = "data/22-09"

client = M100DataClient(dataset_path)

# %%

################################################################################################################################
# Get Jobs
################################################################################################################################

#### Load all job info from the client
df_jobs = client.query("job_info_marconi100")


#### Determine the duration of each job, and filter out all jobs that take less than 120 seconds
df_jobs = df_jobs[df_jobs.nodes.notna()]

df_jobs["duration"] = df_jobs.end_time - df_jobs.start_time
df_jobs["duration_sec"] = df_jobs.duration / pd.Timedelta(seconds=1)
df_jobs = df_jobs[df_jobs["duration_sec"] > 120]

# %%

################################################################################################################################
# Select a single job
################################################################################################################################

job = df_jobs.iloc[0]

nodes = ast.literal_eval(job.nodes) # Get the list off nodes on which the job is run
node = nodes[0]

start_time = job.start_time
end_time = job.end_time

# Round start_time and end_time to days
start_day = start_time.date().strftime('%Y-%m-%d %H:%M:%S')
end_day = ((end_time + pd.Timedelta(days=1)).date()).strftime('%Y-%m-%d %H:%M:%S')


# %%

ganglia_metrics = [
    "cpu_user",
    "cpu_system",
    "cpu_wio"
]

df_ganglia = client.query(ganglia_metrics, tstart=start_day, tstop=end_day, node=str(node))

# %%

df_ganglia_tr = translateDF(df_ganglia, df_ganglia.metric.unique())

df_ganglia_job = df_ganglia_tr[(df_ganglia_tr.timestamp >= start_time) & (df_ganglia_tr.timestamp <= end_time)]

# %%

df_ganglia_tr["cpu_utility"] = df_ganglia_job[ganglia_metrics].sum(axis=1)

plt.plot(df_ganglia_tr.timestamp, df_ganglia_tr["cpu_utility"])

# %%

rolling_power = rollavg(df_ipmi_job.cpu_power, 115)
plt.plot(rolling_power)



# %%

max_cores = 96

df_ganglia_tr["load_one"].hist()

# %%

ipmi_metrics = ["total_power", "p0_power", "p1_power"]

df_ipmi = client.query(ipmi_metrics, tstart=start_day, tstop=end_day, node=str(node))
df_ipmi_tr = translateDF(df_ipmi, df_ipmi.metric.unique())
df_ipmi_tr["cpu_power"] = df_ipmi_tr["p0_power"] + df_ipmi_tr["p1_power"]
df_ipmi_job = df_ipmi_tr[(df_ipmi_tr.timestamp >= start_time) & (df_ipmi_tr.timestamp <= end_time)]



# %%

df_ipmi_job.timestamp.min()

# %%

df_ganglia_job.load_one.plot()

# %%

df_ipmi_job.cpu_power.plot()

# %%

rolling_load = rollavg(df_ganglia_tr.load_one, 23)
rolling_power = rollavg(df_ipmi_tr.cpu_power, 115)

# %%

plotTwo(df_ganglia_tr.timestamp, rolling_load, "load", df_ipmi_tr.timestamp, rolling_power, "power")


# %%

df_ganglia_job[df_ganglia_job["load_one"] <= 20]

# %%

df_ganglia_job["load_one"].hist()

# %%

df_ipmi_job["cpu_power"].hist()

# %% 

df_ipmi_tr_2 = df_ipmi_tr[df_ipmi_tr.timestamp > '2022-09-12 15:00:00+0000']
df_ganglia_tr_2 = df_ganglia_tr[df_ganglia_tr.timestamp > '2022-09-12 15:00:00+0000']



# %%
rolling_load = rollavg(df_ganglia_tr_2.load_one, 23)
rolling_power = rollavg(df_ipmi_tr_2.cpu_power, 115)

# %%

plotTwo(df_ganglia_tr_2.timestamp, rolling_load, "load", df_ipmi_tr_2.timestamp, rolling_power, "power")

# %%
