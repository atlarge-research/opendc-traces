# %% 

import pandas as pd 

from schemas.failure_schema_1 import pa_schema_failure
from write_failure_trace import writeTrace

# %%

df_failure = pd.DataFrame([
    [60_000, 120_000, 0.5],
    [120_000, 180_000, 1.0],
], columns=["failure_interval", "failure_duration", "failure_intensity"])

writeTrace(df_failure, pa_schema_failure, "failure_example.parquet")

# %%

