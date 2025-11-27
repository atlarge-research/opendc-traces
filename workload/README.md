# Workload Traces

Workload Traces define the tasks that need to be simulated by OpenDC. Workload Traces consist of two parquet files: 

- *tasks.parquet:* contains an overview of a tasks including when it is scheduled, its duration, and computational requirements. This is primarily used for scheduling purposes
- *fragments.parquet:* further specifies the computational reqruiremnts over time of a task. 

For more information about how OpenDC uses Carbon Traces see [this](https://atlarge-research.github.io/opendc/docs/documentation/Input/Workload).


The schema of a workload is divided into required and optional columns. The schemas can be found in [schemas].


## Schemas
Over time, the workload schema has been updated multiple times:

- Version 1 -> [OpenDC v2.1](https://github.com/atlarge-research/opendc/releases/tag/v2.1) 

- Version 2 -> [OpenDC v2.3](https://github.com/atlarge-research/opendc/releases/tag/v2.3)

|Schema Version|Introduction|Changes|
|---|---|---|
|v1|[OpenDC v2.1](https://github.com/atlarge-research/opendc/releases/tag/v2.1)||
|v2|[OpenDC v2.3](https://github.com/atlarge-research/opendc/releases/tag/v2.3)|Renamed files to tasks.paquet and fragments.parquet. <br> tasks -> removed stop_time, renamed start_time to submission_time. <br> fragments -> removed timestamp.|
|v3|[PR #342](https://github.com/atlarge-research/opendc/pull/342)|Added support for GPUs|
|v4|[OpenDC v2.4f](https://github.com/atlarge-research/opendc/releases/tag/v2.4f)|Converted id to int. Added Name as an optional parameter as String|
|v4|[OpenDC v2.4g](https://github.com/atlarge-research/opendc/releases/tag/v2.4f)|Renamed the nature column to defferable and turned it into a boolean|

## Converters

[converters.py] contains functions that can be used to convert between different schema versions. 