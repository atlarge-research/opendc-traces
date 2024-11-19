# %%

import pandas as pd
from EnergyData import EnergyData

import pyarrow as pa
import pyarrow.parquet as pq

# %%

start = pd.Timestamp("2024-10-01", tz='Europe/Brussels')
end = pd.Timestamp("2024-10-02", tz='Europe/Brussels')
country_code = 'DE'  # Netherlands

energy_data = EnergyData(start, end, country_code)

df_intensity = energy_data.df[["carbon_intensity"]].copy()
df_intensity["timestamp"] = df_intensity.index
df_intensity = df_intensity.reset_index(drop=True)


schema_intensity = {
    "timestamp": pa.timestamp("ms"),
    "carbon_intensity": pa.float64(),
}

pa_schema_intensity = pa.schema([pa.field(x, y, nullable=False) for x, y in schema_intensity.items()])

pa_intensity_out = pa.Table.from_pandas(
    df = df_intensity,
    schema = pa_schema_intensity,
    preserve_index=False
)

pq.write_table(pa_intensity_out, f"results/opendc/{start.strftime("%Y-%m-%d")}_{end.strftime("%Y-%m-%d")}_{country_code}.parquet")
