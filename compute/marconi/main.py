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


def running_mean(x, N):
    print(x)
    print(np.insert(x, 0, 0))
    cumsum = np.cumsum(np.insert(x, 0, 0)) 

    print(cumsum)
    return (cumsum[N:] - cumsum[:-N]) / float(N)


def rollavg_roll_edges(a,n):
    # see https://stackoverflow.com/questions/42101082/fast-numpy-roll
    'Numpy array rolling, edge handling'
    assert n%2==1
    a = np.pad(a,(0,n-1-n//2), 'constant')*np.ones(n)[:,None]
    m = a.shape[1]
    idx = np.mod((m-1)*np.arange(n)[:,None] + np.arange(m), m) # Rolling index
    out = a[np.arange(-n//2,n//2)[:,None], idx]
    d = np.hstack((np.arange(1,n),np.ones(m-2*n+1+n//2)*n,np.arange(n,n//2,-1)))
    return (out.sum(axis=0)/d)[n//2:]

# %%

#### Start client with specified data

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

################################################################################################################################
# Get all jobs that are run on a specific node
################################################################################################################################

def hasNode(inp, node):
    """Checks if a job is run on a specific node

    Args:
        inp (_type_): _description_
        node (_type_): _description_

    Returns:
        _type_: _description_
    """
    nodes = ast.literal_eval(inp)

    return node in nodes

df_node = df_jobs[df_jobs["nodes"].apply(hasNode, args=(node,))]
df_node[(df_node.end_time > start_time) & (df_node.start_time < end_time)].job_id # Checks which job is running at the node

# %%

################################################################################################################################
# Get hardware utilzation info from the ganglia plugin
################################################################################################################################

ganglia_metrics = [
    # "cpu_system",
    # "cpu_user",
    # "cpu_nice",
    # "Gpu0_gpu_utilization",
    # "Gpu1_gpu_utilization",
    # "Gpu2_gpu_utilization",
    # "Gpu3_gpu_utilization",
    "load_one",
    "load_five",
    "load_fifteen",
    # "proc_run",
    # "proc_total"
    ]

df_ganglia = client.query(ganglia_metrics, tstart=start_day, tstop=end_day, node=str(node))

# %%

df_ganglia = translateDF(df_ganglia, df_ganglia.metric.unique())

df_ganglia_job = df_ganglia[(df_ganglia.timestamp >= start_time) & (df_ganglia.timestamp <= end_time)]

# %%

load_one_rolling = rollavg_roll_edges(df_ganglia_job.load_one, 99)
load_five_rolling = rollavg_roll_edges(df_ganglia_job.load_five, 99)
load_fifteen_rolling = rollavg_roll_edges(df_ganglia_job.load_fifteen, 99)

# %%

proc_run_rolling = rollavg_roll_edges(df_ganglia_job.proc_run, 11)
proc_total_rolling = rollavg_roll_edges(df_ganglia_job.proc_total, 11)

plotTwo(df_ganglia_job.timestamp, proc_run_rolling, "running", 
        df_ganglia_job.timestamp, proc_total_rolling, "total")

# %%

################################################################################################################################
# Get Power and temperature information from the IMPE plugin
################################################################################################################################

# Gather all different CPU metrics
ipmi_metrics = ["total_power"]
for c in client.metrics_per_plugin["ipmi"]:
    if ("p0" in c) or "p1" in c:
        if "temp" in c or "power" in c:
            ipmi_metrics.append(c)

print(ipmi_metrics)

# %%

ipmi_metrics = ['total_power', 'p1_core18_temp', 'p1_core5_temp', 'p0_core12_temp', 
                'p1_power', 'p0_core14_temp', 'p0_core2_temp', 'p0_core9_temp', 
                'p1_core13_temp', 'p0_core18_temp', 'p0_core13_temp', 'p0_core23_temp', 
                'p1_core20_temp', 'p1_core16_temp', 'p0_core5_temp', 'p0_vdd_temp', 
                'p1_core19_temp', 'p1_mem_power', 'p1_core22_temp', 'p0_mem_power', 
                'p1_core7_temp', 'p0_core16_temp', 'p1_core15_temp', 'p0_io_power', 
                'p0_core22_temp', 'p0_core20_temp', 'p0_core0_temp', 'p0_core11_temp', 
                'p1_core23_temp', 'p1_core2_temp', 'p1_core3_temp', 'p0_core8_temp', 
                'p0_core15_temp', 'p0_core1_temp', 'p1_core9_temp', 'p1_core14_temp', 
                'p1_core8_temp', 'p1_core21_temp', 'p0_core4_temp', 'p1_core12_temp', 
                'p1_core0_temp', 'p0_core21_temp', 'p1_core10_temp', 'p0_core3_temp', 
                'p1_core6_temp', 'p0_core7_temp', 'p0_core10_temp', 'p1_core4_temp', 
                'p1_core17_temp', 'p1_core11_temp', 'p0_core17_temp', 'p0_power', 
                'p0_core6_temp', 'p0_core19_temp', 'p1_vdd_temp', 'p1_core1_temp', 
                'p1_io_power']

# %%

df_ipmi = client.query(ipmi_metrics, tstart=start_day, tstop=end_day, node=str(node))


# %%

df_ipmi_tr = translateDF(df_ipmi, df_ipmi.metric.unique())
df_ipmi_tr["avg_core_temp"] = df_ipmi_tr[temp_metrics].mean(axis=1)


# %%

window_size = 51

temp_rolling = rollavg_roll_edges(df_ipmi_tr.avg_core_temp, window_size)
power_rolling = rollavg_roll_edges(df_ipmi_tr.total_power, window_size)

plotTwo(df_ipmi_tr.timestamp, temp_rolling,  "Temperature (C)", df_ipmi_tr.timestamp, power_rolling, "Power (W)")

# %%


# %%

df_ipmi_job = df_ipmi[(df_ipmi.timestamp >= start_time) & (df_ipmi.timestamp <= end_time)]
df_ipmi_job = translateDF(df_ipmi_job, df_ipmi_job.metric.unique())


# %%

df_ipmi_job

# %%

df_ipmi_job

# %%
temp_metrics = [
    'p0_core8_temp',
    'p0_core9_temp',
    'p0_core10_temp',
    'p0_core11_temp',
    'p0_core12_temp',
    'p0_core13_temp',
    'p0_core14_temp',
    'p0_core15_temp',
    'p0_core16_temp',
    'p0_core17_temp',
    'p0_core18_temp',
    'p0_core19_temp',
    'p0_core20_temp',
    'p0_core21_temp',
    'p0_core22_temp',
    'p1_core4_temp',
    'p1_core5_temp',
    'p1_core6_temp',
    'p1_core7_temp',
    'p1_core8_temp',
    'p1_core9_temp',
    'p1_core10_temp',
    'p1_core11_temp',
    'p1_core12_temp',
    'p1_core13_temp',
    'p1_core18_temp',
    'p1_core19_temp',
    'p1_core20_temp',
    'p1_core21_temp',
    'p1_core22_temp']

temp_metrics_0 = [
    'p0_core8_temp',
    'p0_core9_temp',
    'p0_core10_temp',
    'p0_core11_temp',
    'p0_core12_temp',
    'p0_core13_temp',
    'p0_core14_temp',
    'p0_core15_temp',
    'p0_core16_temp',
    'p0_core17_temp',
    'p0_core18_temp',
    'p0_core19_temp',
    'p0_core20_temp',
    'p0_core21_temp',
    'p0_core22_temp']

temp_metrics_1 = [
    'p1_core4_temp',
    'p1_core5_temp',
    'p1_core6_temp',
    'p1_core7_temp',
    'p1_core8_temp',
    'p1_core9_temp',
    'p1_core10_temp',
    'p1_core11_temp',
    'p1_core12_temp',
    'p1_core13_temp',
    'p1_core18_temp',
    'p1_core19_temp',
    'p1_core20_temp',
    'p1_core21_temp',
    'p1_core22_temp']


df_ipmi_job["avg_core_temp"] = df_ipmi_job[temp_metrics].mean(axis=1)
df_ipmi_job["avg_core_temp_0"] = df_ipmi_job[temp_metrics_0].mean(axis=1)
df_ipmi_job["avg_core_temp_1"] = df_ipmi_job[temp_metrics_1].mean(axis=1)


# %%

temp_rolling = rollavg_roll_edges(df_ipmi_job["avg_core_temp_0"], 51)
power_rolling = rollavg_roll_edges(df_ipmi_job["p0_power"], 51)

plotTwo(df_ipmi_job.timestamp, temp_rolling,  "Temperature (C)", df_ipmi_job.timestamp, power_rolling, "Power (W)")

# %%

power_rolling = rollavg_roll_edges(df_ipmi_job.total_power.to_numpy(), 99)
temp_rolling = rollavg_roll_edges(df_ipmi_job.avg_core_temp_0.to_numpy(), 99)

# %%

power_rolling = rollavg_roll_edges(df_ipmi_job.total_power.to_numpy(), 99)
load_one_rolling = rollavg_roll_edges(df_ganglia_job.load_one, 31)

plotTwo(df_ipmi_job.timestamp, power_rolling, "Power (W)", df_ganglia_job.timestamp, load_one_rolling,  "Load One")
# plotTwo(df_ipmi_job.timestamp, power_rolling, "Power (W)", df_ganglia_job.timestamp, load_five_rolling, "Load five")
# plotTwo(df_ipmi_job.timestamp, power_rolling, "Power (W)", df_ganglia_job.timestamp, load_fifteen_rolling, "Load fifteen")



# %%


df_ganglia_job


# %%

temp_metrics_1 = [
    'p0_core8_temp',
    'p0_core9_temp',
    'p0_core10_temp',
    'p0_core11_temp',
    'p0_core12_temp',
    'p0_core13_temp',
    'p0_core14_temp',
    'p0_core15_temp',
    'p0_core16_temp',
    'p0_core17_temp',
    'p0_core18_temp',
    'p0_core19_temp',
    'p0_core20_temp',
    'p0_core21_temp',
    'p0_core22_temp']



# %%

df_ganglia_job = translateDF(df_ganglia_job, df_ganglia_job.metric.unique())

# %%

cpu_metrics = ["cpu_nice", "cpu_system", "cpu_user"]

df_ganglia_job["cpu_sum"] = df_ganglia_job[cpu_metrics].sum(axis=1)


# %%

df_ipmi_job["p0_power"]