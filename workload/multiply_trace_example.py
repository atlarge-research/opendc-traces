# %%

from utils.multiply_trace import multiplyTrace

# %%

base_path = "traces/v6_2/surf_week"
multipliers = [50]

for multiplier in multipliers:
    multiplyTrace(base_path, multiplier, f"{base_path}_x{multiplier}")

# %%
