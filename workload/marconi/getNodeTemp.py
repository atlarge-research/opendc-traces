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

temp_metrics = [f"p{x}_core{y}_temp" for x in range(2) for y in range(23)]
ipmi_metrics = temp_metrics + ['total_power', 'p1_power', 'p0_power', "ambient"]


# %%

dataset_path = "data/22-09"

client = M100DataClient(dataset_path)

# %%

node = "265"
start_day = "2022-09-01 00:00:00"
end_day = "2022-09-08 00:00:00"


# %%

df_ipmi = client.query(ipmi_metrics, tstart=start_day, tstop=end_day, node=str(node))
df_ipmi_tr = translateDF(df_ipmi, df_ipmi.metric.unique())

# %%

temp_columns = [x for x in temp_metrics if x in df_ipmi_tr.columns]

temp_columns_0 = [x for x in temp_columns if "p0" in x]
temp_columns_1 = [x for x in temp_columns if "p1" in x]


# %%

df_ipmi_tr["avg_core_temp"] = df_ipmi_tr[temp_columns].mean(axis=1)
df_ipmi_tr["avg_core_temp_0"] = df_ipmi_tr[temp_columns_0].mean(axis=1)
df_ipmi_tr["avg_core_temp_1"] = df_ipmi_tr[temp_columns_1].mean(axis=1)

# %%

df_ipmi_tr.to_parquet(f"power_temp_node-{node}.parquet")