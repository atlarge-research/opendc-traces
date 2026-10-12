# %%

## Check which workload schema versions a trace on disk matches.
## Only the parquet schemas are read, not the data, so this is fast even for very large traces.

import os

import pyarrow.parquet as pq

from schemas.workload_schema_v1 import workload_schema as workload_schema_v1
from schemas.workload_schema_v2 import workload_schema as workload_schema_v2
from schemas.workload_schema_v3 import workload_schema as workload_schema_v3
from schemas.workload_schema_v3_1 import workload_schema as workload_schema_v3_1
from schemas.workload_schema_v4 import workload_schema as workload_schema_v4
from schemas.workload_schema_v5 import workload_schema as workload_schema_v5
from schemas.workload_schema_v5_1 import workload_schema as workload_schema_v5_1
from schemas.workload_schema_v6 import workload_schema as workload_schema_v6
from schemas.workload_schema_v6_1 import workload_schema as workload_schema_v6_1
from schemas.workload_schema_v6_2 import workload_schema as workload_schema_v6_2

SCHEMAS = {
    "v1": workload_schema_v1,
    "v2": workload_schema_v2,
    "v3": workload_schema_v3,
    "v3_1": workload_schema_v3_1,
    "v4": workload_schema_v4,
    "v5": workload_schema_v5,
    "v5_1": workload_schema_v5_1,
    "v6": workload_schema_v6,
    "v6_1": workload_schema_v6_1,
    "v6_2": workload_schema_v6_2,
}

def _describe(field) -> str:
    return f"{field.type}" if field.nullable else f"{field.type} not null"

'''
Compare the columns of a parquet file to the required and optional columns of a schema.
A column matches when its name, type and nullability are the same as in the schema.
Columns that are not part of the schema are not allowed, because writeTrace would not write them.
'''
def _check_columns(file_schema, required_schema, optional_fields, table) -> list[str]:
    expected = {field.name: field for field in required_schema}
    expected.update(optional_fields)

    problems = []

    for field in required_schema:
        if field.name not in file_schema.names:
            problems.append(f"{table}: missing required column \"{field.name}\"")

    for field in file_schema:
        if field.name not in expected:
            problems.append(f"{table}: column \"{field.name}\" is not part of the schema")
        elif field != expected[field.name]:
            problems.append(f"{table}: column \"{field.name}\" is {_describe(field)}, expected {_describe(expected[field.name])}")

    return problems

'''
Check the trace in the folder at path against one schema version.
Returns a list of problems that explain why the trace does not match. An empty list means it matches.
'''
def check_trace(path, version) -> list[str]:
    if version not in SCHEMAS:
        raise ValueError(f"Unsupported version: {version}. Supported versions are: {', '.join(SCHEMAS)}")

    workload_schema = SCHEMAS[version]
    problems = []

    for table in ["tasks", "fragments"]:
        file_name = f"{workload_schema[f'{table}_name']}.parquet"
        file_path = os.path.join(path, file_name)

        if not os.path.isfile(file_path):
            problems.append(f"{table}: file \"{file_name}\" not found")
            continue

        problems += _check_columns(
            pq.read_schema(file_path),
            workload_schema[f"pa_{table}_schema_required"],
            workload_schema[f"pa_{table}_schema_optional"],
            table,
        )

    return problems

'''
Return every schema version that the trace in the folder at path matches, from oldest to newest.
A trace can match multiple versions, for example when the versions only differ in optional columns
the trace does not use. v6_1 and v6_2 have the same schema (only the unit of "mem_capacity" changed),
so a trace that matches one of them always matches both.
Use check_trace to see why a trace does not match a version.
'''
def validate_trace(path) -> list[str]:
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Trace folder not found: {path}")

    return [version for version in SCHEMAS if not check_trace(path, version)]

# %%
