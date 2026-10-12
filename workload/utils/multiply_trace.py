# %%

import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# %%

'''
Multiply the trace in base_path by making multiplier copies of every task and its fragments,
and write the result to output_path.
Copy c of base task j gets id c*num_tasks + j. The tasks are sorted by submission time,
and the fragments are written in the same order as the tasks.
Only the base trace is kept in memory, the multiplied fragments are streamed to disk.
chunk_size is the number of fragments that are built and written at once. Peak memory is roughly
the base trace plus ~50 bytes per fragment in a chunk, independent of the multiplier.
'''
def multiplyTrace(base_path, multiplier, output_path, chunk_size=10_000_000):
    tasks_base = pq.read_table(f"{base_path}/tasks.parquet")
    fragments_base = pq.read_table(f"{base_path}/fragments.parquet")
    num_tasks = tasks_base.num_rows

    print(f"Base: {num_tasks} tasks, {fragments_base.num_rows} fragments")
    assert (tasks_base.schema.field("id").type == pa.int32()), "task id column must be int32. Trace version needs to be v5_1 or higher"

    # Index of the task each fragment belongs to
    fragment_task_index = pd.Index(tasks_base["id"].to_numpy()).get_indexer(fragments_base["id"].to_numpy())
    assert (fragment_task_index >= 0).all(), "fragments reference unknown task ids"

    # Group the fragments per task. The sort is stable, so the order of fragments within a task is kept.
    if (np.diff(fragment_task_index) < 0).any():
        order = np.argsort(fragment_task_index, kind="stable")
        fragments_base = fragments_base.take(order)
        fragment_task_index = fragment_task_index[order]

    fragment_counts = np.bincount(fragment_task_index, minlength=num_tasks)
    fragment_starts = np.concatenate([[0], np.cumsum(fragment_counts)[:-1]])

    id_field = fragments_base.schema.field("id")
    id_column = fragments_base.schema.get_field_index("id")

    # Copy c of base task j gets id c*num_tasks + j
    copy = np.repeat(np.arange(multiplier), num_tasks)
    task_index = np.tile(np.arange(num_tasks), multiplier)
    submission_time = tasks_base["submission_time"].to_numpy()[task_index]

    # Sort all copies by submission time. Stable, so ties are ordered deterministically.
    order = np.argsort(submission_time, kind="stable")
    task_index = task_index[order]
    new_ids = (copy[order] * num_tasks + task_index).astype("int32")

    os.makedirs(output_path, exist_ok=True)

    tasks_out = tasks_base.take(task_index)
    tasks_out = tasks_out.set_column(tasks_out.schema.get_field_index("id"), tasks_base.schema.field("id"), pa.array(new_ids, pa.int32()))
    pq.write_table(tasks_out, f"{output_path}/tasks.parquet")

    # Write the fragments in the same order as the tasks, in chunks of whole tasks
    out_counts = fragment_counts[task_index]
    out_ends = np.cumsum(out_counts)
    total_fragments = int(out_ends[-1])
    boundaries = np.searchsorted(out_ends, np.arange(chunk_size, total_fragments, chunk_size), side="right")
    boundaries = np.unique(np.concatenate([[0], boundaries, [len(task_index)]]))

    with pq.ParquetWriter(f"{output_path}/fragments.parquet", fragments_base.schema) as writer:
        for start, end in zip(boundaries[:-1], boundaries[1:]):
            counts = out_counts[start:end]
            if counts.sum() == 0:
                continue

            # Row indices into fragments_base: for every task, the range [start, start + count)
            offsets = np.repeat(np.cumsum(counts) - counts, counts)
            rows = np.repeat(fragment_starts[task_index[start:end]], counts) + (np.arange(counts.sum()) - offsets)

            chunk = fragments_base.take(rows)
            chunk = chunk.set_column(id_column, id_field, pa.array(np.repeat(new_ids[start:end], counts), pa.int32()))
            writer.write_table(chunk)

    print(f"x{multiplier}: {len(new_ids)} tasks, {total_fragments} fragments -> {output_path}")

# %%
