# Workload Traces

Workload Traces define the tasks that need to be simulated by OpenDC. Workload Traces consist of two parquet files: 

- *tasks.parquet:* contains an overview of a tasks including when it is scheduled, its duration, and computational requirements. This is primarily used for scheduling purposes
- *fragments.parquet:* further specifies the computational reqruiremnts over time of a task. 

For more information about how OpenDC uses Carbon Traces see [this](https://atlarge-research.github.io/opendc/docs/documentation/Input/Workload).


The schema of a workload is divided into required and optional columns. The schemas can be found in [schemas](schemas).


## Schemas
Over time, the workload schema has been updated multiple times:

|Schema Version|Commit|Date|First Release|Changes|
|---|---|---|--|--|
|v1|[cd0b456](https://github.com/atlarge-research/opendc/commit/cd0b45627f0d8da8c8dc4edde223f3c36e9bcfbf)|2021-04-25|[OpenDC v2](https://github.com/atlarge-research/opendc/releases/tag/v2)|First Parquet Format used. Introduced by OpenDC2 and Capelin.|
|v2|[ea5e79f](https://github.com/atlarge-research/opendc/commit/ea5e79fc77072e6151ee7952581b97e35a2027fb)|2022-05-02|[OpenDC v2.1](https://github.com/atlarge-research/opendc/releases/tag/v2.1)|Renamed all columns. Added "cpu_capacity" column to Tasks. Removed "flops" column from Fragments.|
|v3|[3363df4](https://github.com/atlarge-research/opendc/commit/3363df4c72a064e590ca98f8e01832cfa4e15a3f)|2024-08-27||Changed file names from "meta.parquet and "trace.parquet" to "tasks.parquet" and "fragments.parquet". Removed "inference.parquet".|
|v3_1|[3f05c61](https://github.com/atlarge-research/opendc/commit/3f05c61faeb94a2f1c920d87a6ca8bde34d551e0)|2024-09-05|[OpenDC v2.3](https://github.com/atlarge-research/opendc/releases/tag/v2.3)|Renamed "start_time" column to "submission_time". Replaced the "stop_time" column with "duration" that indicates the duration of the task.|
|v4|[1e35c61](https://github.com/atlarge-research/opendc/commit/1e35c61cd31b8bfb33a6ccbb46b08c0466518e6c)|2025-03-20|[OpenDC v2.4d](https://github.com/atlarge-research/opendc/releases/tag/v2.4d)|Added "nature" column to indicate if a task can be deferred. Added "deadline" column defining the latest time the task can finish without breaking SLA.|
|v5|[0c0cf25](https://github.com/atlarge-research/opendc/commit/0c0cf25616771cd40a9e401edcba4a5e5016f76e)|2025-07-16||Added "parents" and "children" columns to define dependencies between tasks required for workflows. Added "gpu_count" and "gpu_capacity" columns for gpu support.|
|v5_1|[e22c97d](https://github.com/atlarge-research/opendc/commit/e22c97dcca7478d6941b78bdf7cd873bc0d23cdc)|2025-07-22|[OpenDC v2.4f](https://github.com/atlarge-research/opendc/releases/tag/v2.4f)|Changed the type of "id" column from String to Int. Added an optional column "name" that can be used to indicate the name of a task. Removed "timestamp", "cpu_count", "gpu_count" columns and from the fragment file.| 
|v6|[cb95cb2](https://github.com/atlarge-research/opendc/commit/cb95cb2a5b24ae62c33962c988e89daf9a1a3e91)|2025-10-02|[OpenDC v2.4g](https://github.com/atlarge-research/opendc/releases/tag/v2.4g)|Changed the String column "nature" to a boolean "defferable" column.|
|v6_1|[fa1bc73](https://github.com/atlarge-research/opendc/commit/fa1bc73bb462e14d4aa60fa1ddce5d999ed1f127)|2026-08-26||Added an optional "num_fragments" column that indicated the number of fragments in each task. This is used by the EfficentWorkloadLoader.|
|v6_2|[b043c53](https://github.com/atlarge-research/opendc/commit/b043c53494600edd16d982550d6d25ae981ed565)|2026-10-06||Same Schema, but OpenDC now loads mem_capacity as MiB, instead of the kb it was doing before (this was a bug)|

## Converters

[converters.py](utils/converters.py) contains functions that can be used to convert between different schema versions. 
## Validation

[validate_workload_trace.py](utils/validate_workload_trace.py) checks which schema versions a trace matches. Only the parquet schemas are read, so this is fast even for large traces.

```python
from utils.validate_workload_trace import validate_trace, check_trace

validate_trace("traces/v6/surf")         # ['v6', 'v6_1', 'v6_2']
check_trace("traces/v6/surf", "v5_1")    # ['tasks: column "deferrable" is not part of the schema']
```

A trace can match multiple versions, because some versions only differ in optional columns. v6_1 and v6_2 have the same schema (only the unit of "mem_capacity" changed), so they cannot be told apart.
