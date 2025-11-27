# Query tool for M100 Dataset (Parquet)

## Usage
There are two functions:
- ```query```: retrieving a metric or list of metrics.
- ```query_plugins```: retrieving all metrics belonging to a plugin or a list of plugins.

They both have as first, required argument, the metric(s) and plugin(s) to retrieve, respectively.

The rest of the arguments are optional, and behave in the same way for both functions:
- `columns`: columns to retrieve, inferred if None.
- `tstart` and `tstop`: timestamp filtering, with tstart <= timestamp < tstop; timestamp format: `'YYYY-MM-DD HH:mm:SS'`.
- Any other column to filter, passing a single value or list of values to match (e.g. `node='428'`).

It's strongly recommended to specify values for the `year_month` column when using timestamp filtering, as this cuts out unwanted data efficiently.

When working with high volumes of data, it's recommended to query the metrics one at a time, with the `query` function.

### Init

```python
from query_tool import M100DataClient

dataset_path = '/path/dataset/'

client = M100DataClient(dataset_path)
```

### Retrieving available plugins and metrics
```python
# Available plugins
plugins = list(client.metrics_per_plugin.keys())

# Metrics for a given plugin (e.g. IPMI)
ipmi_metrics = client.metrics_per_plugin['ipmi']
```

## Examples
### Querying metrics

#### Single metric, whole month, for some nodes
```python
df = client.query('Gpu0_gpu_temp',
                  year_month='22-07',
                  node=['381','383','385','387'])
```

#### Timestamp filtering and column selection (multiple metrics)
```python
metrics = [f'Gpu{i}_gpu_temp' for i in range(4)]

df = client.query(metrics,
                  columns = ['timestamp', 'value', 'node'],
                  tstart='2022-07-10 00:00:00',
                  tstop='2022-07-20 00:00:00',
                  year_month='22-07',
                  node='381')
```
### Querying plugins
#### All vertiv data on 3 months
```python
df = client.query_plugins('vertiv',
                          year_month=['22-03', '22-04', '22-05'])
```


# ToDo / possible improvements
* Automatize year_month filtering when doing timestamp fiiltering.
* Timestamp filtering support for job_table (submit, start or end).
