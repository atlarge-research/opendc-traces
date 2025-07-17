# %% 

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyarrow as pa

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

idle_power = 50
max_power = 350
max_capacity = 2100 * 48 * 2


# %%

node = 265

# Load the power information
df_power = pd.read_parquet(f"power_temp_node-{node}")
df_power = df_power.sort_values("timestamp").reset_index()
df_power["cpu_power"] = df_power.p0_power + df_power.p1_power


# %%

df_power

# %%

# Create utilization based on the idle and max power
df_power["cpu_util"] = (df_power["cpu_power"] - idle_power) / (max_power - idle_power)

# Create CPU_usage by adding randomness to utilization and multiplying it with the max capacity
bucket_size = 500
num_buckets = int(np.ceil(len(df_power) / bucket_size))

cpu_utils = df_power["cpu_util"].to_numpy()

for i in range(num_buckets):
    val =  np.random.normal(0, 0.03)
    start = i * bucket_size
    end = (i+1) * bucket_size
    if end > len(cpu_utils):
        end = len(cpu_utils)
    

    cpu_utils[start:end] = cpu_utils[start:end] + val

cpu_utils = cpu_utils + np.random.normal(0, 0.03)

cpu_utils = cpu_utils.clip(0,1)

df_power["cpu_usage"] = cpu_utils * max_capacity

# Create a meta trace with a single task
df_meta = pd.DataFrame(
    [["0", df_power.timestamp.min(), df_power.timestamp.max(), 48*2, max_capacity, 100_000_000]],
    columns=["id", "start_time", "stop_time", "cpu_count", "cpu_capacity", "mem_capacity"])

schema_meta = {
    "id": pa.string(),
    "start_time": pa.timestamp("ms"),
    "stop_time": pa.timestamp("ms"),
    "cpu_count": pa.int32(),
    "cpu_capacity": pa.float64(),
    "mem_capacity": pa.int64()
}

pa_schema_meta = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_meta.items()])

pa_meta = pa.Table.from_pandas(
    df = df_meta,
    schema = pa_schema_meta,
    preserve_index=False
)

if not os.path.exists(f"traces/cineca/{node}"):
    os.mkdir(f"traces/cineca/{node}")

if not os.path.exists(f"traces/cineca/{node}/trace"):
    os.mkdir(f"traces/cineca/{node}/trace")

pa.parquet.write_table(pa_meta, f"traces/cineca/{node}/trace/meta.parquet")

# Gather the information for the fragments from the data
timestamps = df_power["timestamp"].to_numpy()
cpu_usages = df_power["cpu_usage"].to_numpy()

durations = (timestamps[1:] - timestamps[:-1])
durations = [int(duration.total_seconds()*1000) for duration in durations]

trace = []

for (timestamp, cpu_usage, duration) in zip(timestamps[1:], cpu_usages[1:], durations):
    trace.append(["0", timestamp, duration, 96, cpu_usage])


df_trace = pd.DataFrame(trace, columns=["id", "timestamp", "duration", "cpu_count", "cpu_usage"])


schema_trace = {
    "id": pa.string(),
    "timestamp": pa.timestamp("ms"),
    "duration": pa.int64(),
    "cpu_count": pa.int32(),
    "cpu_usage": pa.float64()
}

pa_schema_trace = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_trace.items()])

pa_trace = pa.Table.from_pandas(
    df = df_trace,
    schema = pa_schema_trace,
    preserve_index=False
)

print(f"Exporting trace output")
pa.parquet.write_table(pa_trace, f"traces/cineca/{node}/trace/trace.parquet")


# %%
#################################################################################################################################
# Evaluation
#################################################################################################################################

node = 5

df_host = pd.read_parquet(f"output/cineca/raw-output/{node}/seed=0/host.parquet")
df_service = pd.read_parquet(f"output/cineca/raw-output/{node}/seed=0/service.parquet")
df_server = pd.read_parquet(f"output/cineca/raw-output/{node}/seed=0/server.parquet")

df_power = pd.read_parquet(f"traces/cineca/power-temperature-data/power_temp_node-{node+260}")
df_power = df_power.sort_values("timestamp").reset_index()
df_power["cpu_power"] = df_power.p0_power + df_power.p1_power

rolling_data = rollavg(df_power["cpu_power"], 99)
rolling_opendc = rollavg(df_host["power_draw"], 99)

plt.plot(rolling_data, label="data")
plt.plot(rolling_opendc, label="opendc")

plt.legend()
plt.show()
# %%
