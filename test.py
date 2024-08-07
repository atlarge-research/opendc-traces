# %%

import numpy as np
import pandas as pd 
from utils import validateTrace, cleanFragments, addPTS, addCPUprev, getMissingFragmentsBefore, getMissingFragmentsAfter
# from fixMissingFragments import fixMissingFragments
from fixOverlappingFragments import fixOverlappingFragments


# %%

df_meta = pd.read_parquet("traces/bitbrains-small/trace/meta.parquet")
df_fragments = pd.read_parquet("traces/bitbrains-small/trace/trace.parquet")

validateTrace(df_meta, df_fragments)

# %%



df_meta_new, df_fragments_new = fixStartingTime(df_meta, df_fragments, method="meta")
# %%

df_fragments_before = fixOverlappingFragments(df_meta, df_fragments, method="before")
df_fragments_after = fixOverlappingFragments(df_meta, df_fragments, method="after")
df_fragments_mean = fixOverlappingFragments(df_meta, df_fragments, method="mean")

# %%

df_fragments_before = fixMissingFragments(df_meta, df_fragments_before, method="before")

# %%

validateTrace(df_meta, df_fragments_before)