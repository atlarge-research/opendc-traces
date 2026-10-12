## Tests for utils/write_workload_trace.py.
## Run from the workload folder with: python -m pytest

import pandas as pd
import pytest

from schemas.workload_schema_v6_2 import workload_schema as workload_schema_v6_2
from utils.write_workload_trace import toArrowTables

START = pd.Timestamp("2024-01-01 00:00:00")
START_MS = 1_704_067_200_000
HOUR_MS = 3_600_000


def trace(submission_time, duration, fragment_duration):
    df_tasks = pd.DataFrame({
        "id": pd.array([0], dtype="int32"),
        "submission_time": submission_time,
        "duration": duration,
        "cpu_count": pd.array([1], dtype="int32"),
        "cpu_capacity": [1000.0],
        "mem_capacity": [1024],
    })
    df_fragments = pd.DataFrame({
        "id": pd.array([0], dtype="int32"),
        "duration": fragment_duration,
        "cpu_usage": [1000.0],
    })
    return df_tasks, df_fragments


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_times_are_written_as_ms(unit):
    hour = pd.Series([pd.Timedelta(hours=1)]).dt.as_unit(unit)
    submission_time = pd.Series([START]).dt.as_unit(unit)

    tasks, fragments = toArrowTables(*trace(submission_time, hour, hour), workload_schema_v6_2)

    assert tasks["submission_time"].to_pylist() == [START_MS]
    assert tasks["duration"].to_pylist() == [HOUR_MS]
    assert fragments["duration"].to_pylist() == [HOUR_MS]


def test_timezone_aware_times():
    submission_time = pd.Series([START]).dt.tz_localize("Europe/Amsterdam")

    tasks, _ = toArrowTables(*trace(submission_time, [HOUR_MS], [HOUR_MS]), workload_schema_v6_2)

    # Amsterdam is one hour ahead of UTC in January.
    assert tasks["submission_time"].to_pylist() == [START_MS - HOUR_MS]


def test_integer_times_are_kept():
    tasks, fragments = toArrowTables(*trace([START_MS], [HOUR_MS], [HOUR_MS]), workload_schema_v6_2)

    assert tasks["submission_time"].to_pylist() == [START_MS]
    assert tasks["duration"].to_pylist() == [HOUR_MS]
    assert fragments["duration"].to_pylist() == [HOUR_MS]


def test_missing_times_stay_null():
    df_tasks, df_fragments = trace([START_MS], [HOUR_MS], [HOUR_MS])
    df_tasks = pd.concat([df_tasks, df_tasks.assign(id=pd.array([1], dtype="int32"))], ignore_index=True)
    df_tasks["deadline"] = pd.Series([START + pd.Timedelta(hours=2), pd.NaT]).dt.as_unit("us")

    tasks, _ = toArrowTables(df_tasks, df_fragments, workload_schema_v6_2)

    assert tasks["deadline"].to_pylist() == [START_MS + 2 * HOUR_MS, None]


def test_input_is_not_modified():
    hour = pd.Series([pd.Timedelta(hours=1)]).dt.as_unit("us")
    df_tasks, df_fragments = trace(pd.Series([START]).dt.as_unit("us"), hour, hour)
    tasks_before, fragments_before = df_tasks.copy(), df_fragments.copy()

    toArrowTables(df_tasks, df_fragments, workload_schema_v6_2)

    pd.testing.assert_frame_equal(df_tasks, tasks_before)
    pd.testing.assert_frame_equal(df_fragments, fragments_before)
