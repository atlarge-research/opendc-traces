# Workload Traces

Workload Traces define the tasks that need to be simulated by OpenDC. Workload Traces consist of two parquet files: 

- *tasks.parquet:* contains an overview of a tasks including when it is scheduled, its duration, and computational requirements. This is primarily used for scheduling purposes
- *fragments.parquet:* further specifies the computational reqruiremnts over time of a task. 

For more information about how OpenDC uses Carbon Traces see [this](https://atlarge-research.github.io/opendc/docs/documentation/Input/Workload).


The schema of a workload is divided into required and optional columns. The schemas can be found in [schemas].