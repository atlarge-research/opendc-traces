## Tests for utils/multiply_trace.py.
## Run from the workload folder with: python -m pytest

import pandas as pd
import pytest

from schemas.workload_schema_v6_2 import workload_schema as workload_schema_v6_2
from utils.multiply_trace import multiply_trace
from utils.write_workload_trace import toArrowTables


'''
Task 10 is submitted at 100 and has fragments a, b. Task 20 is submitted at 0 and has fragment c.
Task 30 is submitted at 50 and has no fragments.
The fragments of task 10 are not next to each other, to check that they are grouped.
'''
def sample():
    df_tasks = pd.DataFrame({
        "id": pd.array([10, 20, 30], dtype="int32"),
        "submission_time": [100, 0, 50],
        "duration": [2, 1, 1],
        "cpu_count": pd.array([1, 2, 3], dtype="int32"),
        "cpu_capacity": [1000.0, 2000.0, 3000.0],
        "mem_capacity": [1024, 2048, 3072],
    })
    df_fragments = pd.DataFrame({
        "id": pd.array([10, 20, 10], dtype="int32"),
        "duration": [1, 1, 1],
        "cpu_usage": [1.0, 3.0, 2.0],
    })
    return df_tasks, df_fragments


def test_multiply():
    df_tasks, df_fragments = multiply_trace(*sample(), 2)

    # Copy c of the j-th task gets id c * 3 + j. Sorted by submission time, copies of a task stay in order.
    assert df_tasks["id"].tolist() == [1, 4, 2, 5, 0, 3]
    assert df_tasks["submission_time"].tolist() == [0, 0, 50, 50, 100, 100]
    assert df_tasks["cpu_count"].tolist() == [2, 2, 3, 3, 1, 1]

    # Fragments follow the order of the tasks, and keep their order within a task.
    assert df_fragments["id"].tolist() == [1, 4, 0, 0, 3, 3]
    assert df_fragments["cpu_usage"].tolist() == [3.0, 3.0, 1.0, 2.0, 1.0, 2.0]

    # The result can be written as a trace.
    toArrowTables(df_tasks, df_fragments, workload_schema_v6_2)


def test_multiply_by_one_sorts_and_renumbers():
    df_tasks, df_fragments = multiply_trace(*sample(), 1)

    assert df_tasks["id"].tolist() == [1, 2, 0]
    assert df_fragments["id"].tolist() == [1, 0, 0]
    assert df_fragments["cpu_usage"].tolist() == [3.0, 1.0, 2.0]


@pytest.mark.parametrize("multiplier", [1, 3, 7])
def test_every_copy_has_the_same_fragments(multiplier):
    base_tasks, base_fragments = sample()
    df_tasks, df_fragments = multiply_trace(base_tasks, base_fragments, multiplier)

    assert len(df_tasks) == multiplier * len(base_tasks)
    assert len(df_fragments) == multiplier * len(base_fragments)
    assert df_tasks["id"].is_unique
    assert df_tasks["id"].dtype == "int32"
    assert set(df_fragments["id"]) <= set(df_tasks["id"])

    for task_id in df_tasks["id"]:
        base_id = base_tasks["id"].iloc[task_id % len(base_tasks)]
        fragments = df_fragments[df_fragments["id"] == task_id]["cpu_usage"].tolist()
        assert fragments == base_fragments[base_fragments["id"] == base_id]["cpu_usage"].tolist()


def test_dependencies_point_to_the_same_copy():
    df_tasks, df_fragments = sample()
    df_tasks["parents"] = [None, [10], [10, 20]]
    df_tasks["children"] = [[20, 30], [30], None]

    df_tasks, df_fragments = multiply_trace(df_tasks, df_fragments, 2)
    by_id = df_tasks.set_index("id")

    # Copy 0 has ids 0, 1, 2 and copy 1 has ids 3, 4, 5.
    assert by_id.loc[1, "parents"] == [0]
    assert by_id.loc[4, "parents"] == [3]
    assert by_id.loc[5, "parents"] == [3, 4]
    assert by_id.loc[3, "children"] == [4, 5]
    assert by_id.loc[0, "parents"] is None

    toArrowTables(df_tasks, df_fragments, workload_schema_v6_2)


def test_input_is_not_modified():
    df_tasks, df_fragments = sample()
    tasks_before, fragments_before = df_tasks.copy(), df_fragments.copy()

    multiply_trace(df_tasks, df_fragments, 3)

    pd.testing.assert_frame_equal(df_tasks, tasks_before)
    pd.testing.assert_frame_equal(df_fragments, fragments_before)


@pytest.mark.parametrize("multiplier", [0, -1, 1.5])
def test_invalid_multiplier(multiplier):
    with pytest.raises(ValueError):
        multiply_trace(*sample(), multiplier)


def test_unknown_task_id():
    df_tasks, df_fragments = sample()
    df_fragments.loc[0, "id"] = 99

    with pytest.raises(ValueError):
        multiply_trace(df_tasks, df_fragments, 2)
