# %%

from utils.multiply_trace import multiplyTrace

# %%

base_path = "traces/v6_2/surf"
multipliers = [5]

for multiplier in multipliers:
    multiplyTrace(base_path, multiplier, f"{base_path}_x{multiplier}")

# %%
