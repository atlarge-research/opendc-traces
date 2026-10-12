## Tests for the validator in utils/validate_workload_trace.py.
## Run from the workload folder with: python -m pytest

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from test_converters import VERSIONS, sample
from utils.validate_workload_trace import check_trace, validate_trace
from utils.write_workload_trace import writeTrace

# The versions a trace with only the required columns matches.
# Versions that only add optional columns can not be told apart without those columns.
MATCHES = {
    "v1": ["v1"],
    "v2": ["v2"],
    "v3": ["v3"],
    "v3_1": ["v3_1", "v4", "v5"],
    "v4": ["v3_1", "v4", "v5"],
    "v5": ["v3_1", "v4", "v5"],
    "v5_1": ["v5_1", "v6", "v6_1", "v6_2"],
    "v6": ["v5_1", "v6", "v6_1", "v6_2"],
    "v6_1": ["v5_1", "v6", "v6_1", "v6_2"],
    "v6_2": ["v5_1", "v6", "v6_1", "v6_2"],
}


def add_column(file_path, field, values):
    table = pq.read_table(file_path)
    pq.write_table(table.append_column(field, pa.array(values, type=field.type)), file_path)


@pytest.mark.parametrize("version", VERSIONS)
def test_written_trace_matches_its_version(version, tmp_path):
    writeTrace(*sample(version), version, tmp_path)

    assert validate_trace(tmp_path) == MATCHES[version]


# Optional columns are only part of some versions, so using them narrows down the matches.
OPTIONAL_COLUMNS = [
    ("v4", {"nature": ["deferrable", None]}, ["v4", "v5"]),
    ("v5", {"parents": [None, ["0"]], "gpu_count": [0, 1]}, ["v5"]),
    ("v5_1", {"nature": ["deferrable", None]}, ["v5_1"]),
    ("v5_1", {"name": ["task-a", "task-b"]}, ["v5_1", "v6", "v6_1", "v6_2"]),
    ("v6", {"deferrable": [True, False]}, ["v6", "v6_1", "v6_2"]),
    ("v6_1", {"num_fragments": [2, 1]}, ["v6_1", "v6_2"]),
    ("v6_2", {"parents": [None, [0]], "children": [[1], None]}, ["v5_1", "v6", "v6_1", "v6_2"]),
]


@pytest.mark.parametrize(
    "version,columns,expected",
    OPTIONAL_COLUMNS,
    ids=[f"{version}-{'-'.join(columns)}" for version, columns, _ in OPTIONAL_COLUMNS],
)
def test_optional_columns(version, columns, expected, tmp_path):
    df_tasks, df_fragments = sample(version)
    writeTrace(df_tasks.assign(**columns), df_fragments, version, tmp_path)

    assert validate_trace(tmp_path) == expected


def test_wrong_type(tmp_path):
    writeTrace(*sample("v6_2"), "v6_2", tmp_path)
    table = pq.read_table(tmp_path / "tasks.parquet")
    schema = table.schema.set(0, pa.field("id", pa.int64(), nullable=False))
    pq.write_table(table.cast(schema), tmp_path / "tasks.parquet")

    assert validate_trace(tmp_path) == []
    assert check_trace(tmp_path, "v6_2") == ["tasks: column \"id\" is int64 not null, expected int32 not null"]


def test_unknown_column(tmp_path):
    writeTrace(*sample("v6_2"), "v6_2", tmp_path)
    add_column(tmp_path / "fragments.parquet", pa.field("flops", pa.int64()), [1, 2, 3])

    assert validate_trace(tmp_path) == []
    assert check_trace(tmp_path, "v6_2") == ["fragments: column \"flops\" is not part of the schema"]


def test_missing_required_column(tmp_path):
    writeTrace(*sample("v6_2"), "v6_2", tmp_path)
    table = pq.read_table(tmp_path / "tasks.parquet")
    pq.write_table(table.drop_columns(["cpu_capacity"]), tmp_path / "tasks.parquet")

    assert validate_trace(tmp_path) == []
    assert check_trace(tmp_path, "v6_2") == ["tasks: missing required column \"cpu_capacity\""]


def test_nullable_required_column(tmp_path):
    # pandas writes every column as nullable, while OpenDC expects required columns to be not null.
    df_tasks, df_fragments = sample("v6_2")
    df_tasks.to_parquet(tmp_path / "tasks.parquet")
    df_fragments.to_parquet(tmp_path / "fragments.parquet")

    assert validate_trace(tmp_path) == []
    assert "tasks: column \"id\" is int32, expected int32 not null" in check_trace(tmp_path, "v6_2")


def test_missing_files(tmp_path):
    writeTrace(*sample("v6_2"), "v6_2", tmp_path)
    (tmp_path / "fragments.parquet").unlink()

    assert validate_trace(tmp_path) == []
    assert check_trace(tmp_path, "v6_2") == ["fragments: file \"fragments.parquet\" not found"]
    assert check_trace(tmp_path, "v1") == [
        "tasks: file \"trace/meta.parquet\" not found",
        "fragments: file \"trace/trace.parquet\" not found",
    ]


def test_empty_folder(tmp_path):
    assert validate_trace(tmp_path) == []


def test_missing_folder(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_trace(tmp_path / "missing")


def test_unknown_version(tmp_path):
    with pytest.raises(ValueError):
        check_trace(tmp_path, "v7")
