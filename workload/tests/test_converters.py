## Tests for the converters in utils/converters.py.
## Run from the workload folder with: python -m pytest

import importlib
import warnings

import pandas as pd
import pytest

from utils import converters
from utils.write_workload_trace import toArrowTables

# Most tests drop columns on purpose. The warnings themselves are tested in their own section.
pytestmark = pytest.mark.filterwarnings("ignore::utils.converters.DataLossWarning")

VERSIONS = converters.VERSIONS

UPGRADES = list(zip(VERSIONS, VERSIONS[1:]))
DOWNGRADES = [(new, old) for old, new in UPGRADES]
STEPS = UPGRADES + DOWNGRADES
PAIRS = [(src, dst) for src in VERSIONS for dst in VERSIONS]

T0 = pd.Timestamp("2024-01-01 00:00:00")
HOUR = pd.Timedelta(hours=1)
HOUR_MS = 3_600_000


def schema(version):
    return importlib.import_module(f"schemas.workload_schema_{version}").workload_schema


def converter(src, dst):
    return getattr(converters, f"convert_{src}_to_{dst}")


def ids(pairs):
    return [f"{src}->{dst}" for src, dst in pairs]


'''
The same small trace, written in the given schema version.
Task 0 is submitted at T0 and has two fragments of one hour.
Task 1 is submitted half an hour later and has one fragment of one hour.
'''
def sample(version):
    submission = [T0, T0 + HOUR / 2]
    stop = [T0 + 2 * HOUR, T0 + 1.5 * HOUR]
    submission_ms = [int(t.timestamp() * 1000) for t in submission]
    duration = [2 * HOUR_MS, HOUR_MS]
    cpu_count = pd.array([1, 2], dtype="int32")
    # Equal to the highest cpu usage of each task, which is what v1 -> v2 assumes.
    cpu_capacity = [1000.0, 1500.0]
    # In KiB, multiples of both 1000 and 1024 so the conversions to v1 and v6_2 are exact.
    mem_capacity = [2_048_000, 4_096_000]

    fragment_ids = ["0", "0", "1"]
    fragment_duration = [HOUR_MS, HOUR_MS, HOUR_MS]
    fragment_cpu_count = pd.array([1, 1, 2], dtype="int32")
    fragment_cpu_usage = [500.0, 1000.0, 1500.0]
    # From v2 on, a fragment's timestamp is its end. In v1, its time is its start.
    fragment_timestamp = [T0 + HOUR, T0 + 2 * HOUR, T0 + 1.5 * HOUR]
    fragment_time_v1 = [T0, T0 + HOUR, T0 + HOUR / 2]

    if version == "v1":
        df_tasks = pd.DataFrame({
            "id": ["0", "1"],
            "submissionTime": submission,
            # The start of the last fragment of each task.
            "endTime": [T0 + HOUR, T0 + HOUR / 2],
            "maxCores": cpu_count,
            "requiredMemory": [m // 1000 for m in mem_capacity],
        })
        df_fragments = pd.DataFrame({
            "id": fragment_ids,
            "time": fragment_time_v1,
            "duration": fragment_duration,
            "cores": fragment_cpu_count,
            "cpuUsage": fragment_cpu_usage,
            "flops": [1_800_000, 3_600_000, 5_400_000],
        })
    elif version in ("v2", "v3"):
        df_tasks = pd.DataFrame({
            "id": ["0", "1"],
            "start_time": submission,
            "stop_time": stop,
            "cpu_count": cpu_count,
            "cpu_capacity": cpu_capacity,
            "mem_capacity": mem_capacity,
        })
        df_fragments = pd.DataFrame({
            "id": fragment_ids,
            "timestamp": fragment_timestamp,
            "duration": fragment_duration,
            "cpu_count": fragment_cpu_count,
            "cpu_usage": fragment_cpu_usage,
        })
    elif version in ("v3_1", "v4", "v5"):
        df_tasks = pd.DataFrame({
            "id": ["0", "1"],
            "submission_time": submission_ms,
            "duration": duration,
            "cpu_count": cpu_count,
            "cpu_capacity": cpu_capacity,
            "mem_capacity": mem_capacity,
        })
        df_fragments = pd.DataFrame({
            "id": fragment_ids,
            "timestamp": fragment_timestamp,
            "duration": fragment_duration,
            "cpu_count": fragment_cpu_count,
            "cpu_usage": fragment_cpu_usage,
        })
    else:
        df_tasks = pd.DataFrame({
            "id": pd.array([0, 1], dtype="int32"),
            "submission_time": submission_ms,
            "duration": duration,
            "cpu_count": cpu_count,
            "cpu_capacity": cpu_capacity,
            "mem_capacity": [m // 1024 for m in mem_capacity] if version == "v6_2" else mem_capacity,
        })
        df_fragments = pd.DataFrame({
            "id": pd.array([0, 0, 1], dtype="int32"),
            "duration": fragment_duration,
            "cpu_usage": fragment_cpu_usage,
        })

    return df_tasks, df_fragments


'''
Check that two traces are equal after converting them to the given schema version,
the same way writeTrace does before writing them.
'''
def assert_trace_equal(actual, expected, version):
    actual_tasks, actual_fragments = toArrowTables(*actual, schema(version))
    expected_tasks, expected_fragments = toArrowTables(*expected, schema(version))

    assert actual_tasks.to_pydict() == expected_tasks.to_pydict()
    assert actual_fragments.to_pydict() == expected_fragments.to_pydict()


'''
Add every optional column of the given schema version, filled with nulls.
'''
def with_empty_optional_columns(df_tasks, df_fragments, version):
    workload_schema = schema(version)
    df_tasks = df_tasks.assign(**{column: None for column in workload_schema["pa_tasks_schema_optional"]})
    df_fragments = df_fragments.assign(**{column: None for column in workload_schema["pa_fragments_schema_optional"]})
    return df_tasks, df_fragments


#################################################################################################
## Single steps
#################################################################################################

@pytest.mark.parametrize("src,dst", STEPS, ids=ids(STEPS))
def test_step(src, dst):
    assert_trace_equal(converter(src, dst)(*sample(src)), sample(dst), dst)


@pytest.mark.parametrize("src,dst", STEPS, ids=ids(STEPS))
def test_step_with_empty_optional_columns(src, dst):
    df_tasks, df_fragments = with_empty_optional_columns(*sample(src), src)
    tasks_before, fragments_before = df_tasks.copy(), df_fragments.copy()

    output = converter(src, dst)(df_tasks, df_fragments)

    # The output can be written in the new version, and the input is not modified.
    toArrowTables(*output, schema(dst))
    pd.testing.assert_frame_equal(df_tasks, tasks_before)
    pd.testing.assert_frame_equal(df_fragments, fragments_before)


#################################################################################################
## Chaining
#################################################################################################

@pytest.mark.parametrize("src,dst", PAIRS, ids=ids(PAIRS))
def test_convert(src, dst):
    assert_trace_equal(converters.convert(*sample(src), src, dst), sample(dst), dst)


def test_converter_lists_match_versions():
    for i, (old, new) in enumerate(UPGRADES):
        assert converters.UPGRADERS[i] is converter(old, new)
        assert converters.DOWNGRADERS[i] is converter(new, old)


def test_get_converters_upgrade():
    assert converters.get_converters("v1", "v4") == [
        converters.convert_v1_to_v2,
        converters.convert_v2_to_v3,
        converters.convert_v3_to_v3_1,
        converters.convert_v3_1_to_v4,
    ]


def test_get_converters_downgrade():
    assert converters.get_converters("v5", "v3") == [
        converters.convert_v5_to_v4,
        converters.convert_v4_to_v3_1,
        converters.convert_v3_1_to_v3,
    ]


def test_get_converters_same_version():
    assert converters.get_converters("v4", "v4") == []


def test_get_converters_unknown_version():
    with pytest.raises(ValueError):
        converters.get_converters("v1", "v7")


#################################################################################################
## Specific conversions
#################################################################################################

def test_v1_times_as_epoch_ms():
    # Traces written by OpenDC v2.0 store times as epoch milliseconds instead of timestamps.
    df_tasks, df_fragments = sample("v1")
    for column in ["submissionTime", "endTime"]:
        df_tasks[column] = df_tasks[column].dt.as_unit("ms").astype("int64")
    df_fragments["time"] = df_fragments["time"].dt.as_unit("ms").astype("int64")

    assert_trace_equal(converters.convert_v1_to_v2(df_tasks, df_fragments), sample("v2"), "v2")


def test_v3_to_v3_1_timezone_aware():
    df_tasks, df_fragments = sample("v3")
    for column in ["start_time", "stop_time"]:
        df_tasks[column] = df_tasks[column].dt.tz_localize("UTC")

    assert_trace_equal(converters.convert_v3_to_v3_1(df_tasks, df_fragments), sample("v3_1"), "v3_1")


def test_v5_to_v5_1_keep_name():
    df_tasks, df_fragments = sample("v5")
    df_tasks["id"] = ["task-a", "task-b"]
    df_fragments["id"] = ["task-a", "task-a", "task-b"]

    tasks_v5_1, fragments_v5_1 = converters.convert_v5_to_v5_1(df_tasks, df_fragments, keep_name=True)

    assert tasks_v5_1["id"].tolist() == [0, 1]
    assert tasks_v5_1["name"].tolist() == ["task-a", "task-b"]
    assert fragments_v5_1["id"].tolist() == [0, 0, 1]

    # Converting back uses the names as ids again.
    assert_trace_equal(converters.convert_v5_1_to_v5(tasks_v5_1, fragments_v5_1), (df_tasks, df_fragments), "v5")


def test_v5_1_to_v5_missing_names():
    df_tasks, df_fragments = sample("v5_1")
    df_tasks["name"] = ["task-a", None]

    tasks_v5, fragments_v5 = converters.convert_v5_1_to_v5(df_tasks, df_fragments)

    assert tasks_v5["id"].tolist() == ["task-a", "1"]
    assert fragments_v5["id"].tolist() == ["task-a", "task-a", "1"]


def test_dependencies_follow_new_ids():
    df_tasks, df_fragments = sample("v5")
    df_tasks["id"] = ["task-a", "task-b"]
    df_fragments["id"] = ["task-a", "task-a", "task-b"]
    df_tasks["parents"] = [None, ["task-a"]]
    df_tasks["children"] = [["task-b"], None]

    tasks_v5_1, fragments_v5_1 = converters.convert_v5_to_v5_1(df_tasks, df_fragments, keep_name=True)

    assert tasks_v5_1["parents"].tolist() == [None, [0]]
    assert tasks_v5_1["children"].tolist() == [[1], None]
    toArrowTables(tasks_v5_1, fragments_v5_1, schema("v5_1"))

    assert_trace_equal(converters.convert_v5_1_to_v5(tasks_v5_1, fragments_v5_1), (df_tasks, df_fragments), "v5")


def test_nature_and_deferrable():
    df_tasks, df_fragments = sample("v5_1")
    df_tasks["nature"] = ["deferrable", None]

    tasks_v6, fragments_v6 = converters.convert_v5_1_to_v6(df_tasks, df_fragments)

    assert tasks_v6["deferrable"].tolist() == [True, False]
    assert "nature" not in tasks_v6.columns

    assert_trace_equal(converters.convert_v6_to_v5_1(tasks_v6, fragments_v6), (df_tasks, df_fragments), "v5_1")


def test_mem_capacity_rounds_down():
    df_tasks, df_fragments = sample("v6_1")
    df_tasks["mem_capacity"] = [1500, 2048]

    tasks_v6_2, _ = converters.convert_v6_1_to_v6_2(df_tasks, df_fragments)

    assert tasks_v6_2["mem_capacity"].tolist() == [1, 2]


#################################################################################################
## Data loss warnings
#################################################################################################

# Every column a converter drops because it is not supported in the version it converts to.
DROPPED_COLUMNS = [
    ("v1", "v2", "fragments", "flops"),
    ("v5", "v5_1", "fragments", "timestamp"),
    ("v5", "v5_1", "fragments", "cpu_count"),
    ("v5", "v5_1", "fragments", "gpu_count"),
    ("v6_1", "v6", "tasks", "num_fragments"),
    ("v5", "v4", "tasks", "parents"),
    ("v5", "v4", "tasks", "children"),
    ("v5", "v4", "tasks", "gpu_count"),
    ("v5", "v4", "tasks", "gpu_capacity"),
    ("v5", "v4", "fragments", "gpu_count"),
    ("v5", "v4", "fragments", "gpu_usage"),
    ("v4", "v3_1", "tasks", "nature"),
    ("v4", "v3_1", "tasks", "deadline"),
    ("v2", "v1", "tasks", "cpu_capacity"),
]


@pytest.mark.parametrize(
    "src,dst,table,column",
    DROPPED_COLUMNS,
    ids=[f"{src}->{dst}-{table}.{column}" for src, dst, table, column in DROPPED_COLUMNS],
)
def test_dropping_data_warns(src, dst, table, column):
    df_tasks, df_fragments = sample(src)
    if table == "tasks":
        df_tasks[column] = 1
    else:
        df_fragments[column] = 1

    with pytest.warns(converters.DataLossWarning, match=f"{table} column.*{column}"):
        converter(src, dst)(df_tasks, df_fragments)


@pytest.mark.parametrize("src,dst", [("v6_1", "v6"), ("v5", "v4"), ("v4", "v3_1")], ids=ids([("v6_1", "v6"), ("v5", "v4"), ("v4", "v3_1")]))
def test_dropping_empty_columns_does_not_warn(src, dst):
    df_tasks, df_fragments = with_empty_optional_columns(*sample(src), src)

    with warnings.catch_warnings():
        warnings.simplefilter("error", converters.DataLossWarning)
        converter(src, dst)(df_tasks, df_fragments)


def test_v5_to_v5_1_warns_when_ids_are_lost():
    df_tasks, df_fragments = sample("v5")
    df_tasks["id"] = ["task-a", "task-b"]
    df_fragments["id"] = ["task-a", "task-a", "task-b"]

    with pytest.warns(converters.DataLossWarning, match="keep_name"):
        converters.convert_v5_to_v5_1(df_tasks, df_fragments)


@pytest.mark.parametrize("keep_name,task_ids", [(True, ["task-a", "task-b"]), (False, ["0", "1"])])
def test_v5_to_v5_1_no_warning_when_ids_are_kept(keep_name, task_ids):
    df_tasks, df_fragments = sample("v5")
    df_tasks["id"] = task_ids
    df_fragments["id"] = [task_ids[0], task_ids[0], task_ids[1]]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        converters.convert_v5_to_v5_1(df_tasks, df_fragments, keep_name=keep_name)

    assert not any("keep_name" in str(warning.message) for warning in caught)
