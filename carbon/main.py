# %%

import numpy as np
import pandas as pd
from EnergyData import EnergyData

# %%

df = pd.read_pickle("pickles/2022-01-01 00:00:00+01:00_2022-12-31 00:00:00+01:00_NL.pkl")

# %%
start = pd.Timestamp("2022-01-1", tz='Europe/Brussels')
end = pd.Timestamp("2022-12-31", tz='Europe/Brussels')
country_code = 'NL'  # Netherlands

energy_data = EnergyData(start, end, country_code)
# %%


energy_data.df

# %%

df_intensity = energy_data.df[["carbon_intensity"]]
df_intensity["timestamp"] = df_intensity.index
df_intensity = df_intensity.reset_index(drop=True)

# %%

df_intensity.to_parquet("carbon_2022.parquet")


# %%

df_meta = pd.read_parquet("meta.parquet")

# %%

df = pd.read_parquet("carbon_intensity.parquet")

# %%
df_intensity.time.isna()

# %%

df