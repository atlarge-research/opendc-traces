# %%

import pandas as pd
from datetime import datetime 
from TraceTools.TraceWriter.writeTrace import writeTrace
from TraceTools.TraceWriter.variables import meta_columns, fragments_columns

# %%

output_folder = "traces/incorrect"



# create meta
meta = [
    ["correct", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["missing", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["overlap", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["overlap2", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["start", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
    ["stop", datetime.strptime("2024-2-1 10:00", "%Y-%m-%d %H:%M"), datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 4, 80000, 100_000,],
]

df_meta = pd.DataFrame(meta, columns=meta_columns)

# create fragment
fragments = [
    ["correct", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 1000],
    ["correct", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 2000],

    ["missing", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 2000],
    ["missing", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), (60*60-1)*1000, 1, 3000],

    ["overlap", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 3000],
    ["overlap", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 4000],
    ["overlap", datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 4000],

    ["overlap2", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 5000],
    ["overlap2", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 6000],
    ["overlap2", datetime.strptime("2024-2-1 13:00", "%Y-%m-%d %H:%M"), (60*60+1)*1000, 1, 7000],

    ["start", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), (60*60 - 1)*1000, 1, 4000],
    ["start", datetime.strptime("2024-2-1 12:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 5000],

    ["stop", datetime.strptime("2024-2-1 11:00", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 5000],
    ["stop", datetime.strptime("2024-2-1 12:01", "%Y-%m-%d %H:%M"), 60*60*1000, 1, 6000],
]

df_fragments = pd.DataFrame(fragments, columns=fragments_columns)


# %%


writeTrace(df_meta, df_fragments, "incorrect")


# %%
