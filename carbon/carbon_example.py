# %% 

import pandas as pd 

from schemas.carbon_schema_1 import pa_schema_carbon
from carbon.utils.write_carbon_trace import writeTrace

# %%

start_time = pd.to_datetime("2024-01-01", utc=True)
start_time_ms = int(start_time.timestamp() * 1000)

# %%

df_carbon = pd.DataFrame([
    [start_time_ms, 100.0],
    [start_time_ms + 3600*1000, 90.0],
], columns=["timestamp", "carbon_intensity"])

writeTrace(df_carbon, pa_schema_carbon, "carbon_example.parquet")

# %%

