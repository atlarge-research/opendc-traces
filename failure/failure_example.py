# %% 

import pandas as pd 

from schemas.failure_schema_1 import pa_schema_failure
from failure.utils.write_failure_trace import writeTrace

# %%

df_failure = pd.DataFrame([
    [3_600_0000, 3600_000, 1.0],
], columns=["failure_interval", "failure_duration", "failure_intensity"])

writeTrace(df_failure, pa_schema_failure, "failure_10h.parquet")

# %%

