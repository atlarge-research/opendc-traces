import os
import datetime
import pickle
from collections import defaultdict
import pyarrow as pa
import pyarrow.dataset as ds
import pandas as pd


class M100DataClient():
    def __init__(self, path):
        """
        Initializes the client.

        Args:
            path (string): path of the Parquet dataset (entrypoint).
        """
        
        # dataset path
        self.path=path

        # loading metadata
        script_path = os.path.dirname(os.path.realpath(__file__))

        self.metrics_per_plugin = pickle.load(open(script_path+'/schema_metadata/metrics_per_plugin.pkl', 'rb'))
        self.tags_per_metric = pickle.load(open(script_path+'/schema_metadata/tags_per_metric.pkl', 'rb'))
        self.dtype_per_metric = pickle.load(open(script_path+'/schema_metadata/dtype_per_metric.pkl', 'rb'))
        self.part_cols_dictionaries = pickle.load(open(script_path+'/schema_metadata/part_cols_dictionaries.pkl', 'rb'))
        self.dict_cols = pickle.load(open(script_path+'/schema_metadata/dict_cols.pkl', 'rb'))
        self.schema_stump = pickle.load(open(script_path+'/schema_metadata/common_schema.pkl', 'rb'))

        self.all_tags = self.schema_stump.names

        self.value_dtypes = [pa.int32(), pa.int64(),
                             pa.float32(), pa.float64(),
                             pa.string(), None] # None for job_table, has no "value" column
        

        # creating a pyarrow.dataset.Dataset per possible dtype of the "value" column (different schemas)
        self.dataset_per_dtype = {}

        parquet_format = ds.ParquetFileFormat(read_options={'dictionary_columns': self.dict_cols})
        
        for dtype in self.value_dtypes:
            if dtype is None:
                schema = self.schema_stump
            elif dtype == pa.string():
                schema = self.schema_stump.append(pa.field('value', pa.dictionary(pa.int32(), pa.string())))
            else:
                schema = self.schema_stump.append(pa.field('value', dtype))

            part = ds.partitioning(schema,
                                   flavor='hive',
                                   dictionaries=self.part_cols_dictionaries)

            self.dataset_per_dtype[dtype] = ds.dataset(self.path,
                                                       format=parquet_format,
                                                       partitioning=part,
                                                       schema=schema)

    
    def query(self, metrics, columns=None, tstart=None, tstop=None, **kwargs):
        """
        Queries the dataset for specific metrics.
        A list of metrics (or a single one) has to be specified.
        If names of columns are specified, those are retrieved; if not specified,
        all columns in the union of the requested metrics are retrieved.
        Timestamp filtering is supported, retrieving data with  tstart <= timestamp < tstop.
        The required format format for the two arguments (timestamp) is: "YYYY-MM-DD HH:mm:SS".
        Any ulterior column can be specified for filtering, passing a single value or a list:
        only samples with values of that column in that list (or equal to that value) are retrieved.

        When doing filtering on the timestamp, filtering also for the "year_month" column is recommended,
        as it can be done trivially.

        Args:
            metrics (string or List[string]): metrics to be retrieved.
            columns (List[string], optional): columns to be retrieved, inferred if None. Defaults to None.
            tstart (string, optional): start timestamp, format: "YYYY-MM-DD HH:mm:SS". Defaults to None.
            tstop (string, optional): end timestamp, format: "YYYY-MM-DD HH:mm:SS". Defaults to None.
            **kwargs: columns to filter, passing for each columns (key) a value or list of values to match.

        Returns:
            pd.DataFrame: DataFrame with requested data.
        """
        
        # TODO: handle job_table timestamp filtering (has no 'timestamp', but other datetime fields)
        # input sanitization
        #if 'plugin' in kwargs.keys():
        #    raise AttributeError("'plugin' tag not allowed in this function. Use 'query_plugins' instead.")
        
        for key, values in kwargs.items():
            if key not in self.all_tags:
                raise AttributeError(f'"{key}" is not a valid tag.')
            if not isinstance(values, list):
                kwargs[key] = [values]
        
        if not isinstance(metrics, list):
            metrics = [metrics]
            
        if columns == None:
            tags_per_metric_sets = set(metric_tags 
                                       for metric in metrics
                                       for metric_tags in self.tags_per_metric[metric])
            columns = list(set.union(tags_per_metric_sets))
            
        timestamp_filtering = False
        if tstart or tstop:
            # both have to be specificed if one of them is
            if not tstart or not tstop:
                raise AttributeError('If tstart or tstop is specified, both have to be specified.')
                
            timestamp_filtering = True
                
        # grouping metrics by dtype
        metrics_per_dtype = self._get_metrics_per_dtype(metrics)
        unique_dtypes = list(metrics_per_dtype.keys())

        # common filters among queries ('metric' filter excluded)
        common_filter = None
        if len(kwargs)>0:
            keys = list(kwargs.keys())
            all_values = list(kwargs.values())
            
            common_filter = ds.field(keys[0]).isin(all_values[0])
            
            if len(kwargs)>1:
                for key, values in zip(keys[1:], all_values[1:]):
                    common_filter &= ds.field(key).isin(values)
                    
        # adding timestamp filtering (if requested); right endpoint excluded
        if timestamp_filtering:
            dt_start = datetime.datetime.strptime(tstart, '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc)
            dt_stop = datetime.datetime.strptime(tstop, '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc)
            
            timestamp_filter = (ds.field('timestamp').cast(pa.timestamp('ms', tz='UTC')) >= dt_start)
            timestamp_filter &= (ds.field('timestamp').cast(pa.timestamp('ms', tz='UTC')) < dt_stop) # exclusive

            if common_filter is not None:
                common_filter &= timestamp_filter
            else:
               common_filter = timestamp_filter

        tables = []
        # do a query for metrics of each dtype
        for dtype in unique_dtypes:
            print(f'Retrieving data of type: {dtype}')
            dtype_metrics = metrics_per_dtype[dtype]
            dtype_metrics_filter = ds.field('metric').isin(dtype_metrics)
            if common_filter is not None:
                dtype_metrics_filter &= common_filter
                
            
            table = self.dataset_per_dtype[dtype].to_table(columns=columns,
                                                           filter=dtype_metrics_filter)
            tables.append(table)
        
        # concat tables (PyArrow)
        #return self._concat_tables_pyarrow(tables)
        
        # convert tables to pandas and concatenate them (using Pandas type inference)
        return self._concat_tables_pandas(tables)
                           
    def query_plugins(self, plugins, **kwargs):
        """
        See query() arguments,
        the difference being that a plugin or list of plugins has to be specified instead of metrics.
        """
        
        if 'metric' in kwargs.keys():
            raise AttributeError("'metric' tag not allowed in this function. Use 'query' instead.")
        
        if not isinstance(plugins, list):
            plugins = [plugins]
        
        metrics = [metric for plugin in plugins for metric in self.metrics_per_plugin[plugin]]
        df = self.query(metrics, **kwargs)

        return df
        
    def _get_metrics_per_dtype(self, metrics):
        
        metrics_per_dtype = defaultdict(list)
        for metric in metrics:
            dtype = self.dtype_per_metric[metric]
            metrics_per_dtype[dtype].append(metric)
            
        return metrics_per_dtype

    def _concat_tables_pandas(self, tables):
        """
        Concatenating tables with different dtypes for 'value', using a single dtype.
        Pandas version.
        """
        # using Pandas nullable dtypes, to prevent conversion to float when NaNs
        dtype_mapping = {pa.int8(): pd.Int8Dtype(),
                         pa.int16(): pd.Int16Dtype(),
                         pa.int32(): pd.Int32Dtype(),
                         pa.int64(): pd.Int64Dtype(),
                         pa.uint8(): pd.UInt8Dtype(),
                         pa.uint16(): pd.UInt16Dtype(),
                         pa.uint32(): pd.UInt32Dtype(),
                         pa.uint64(): pd.UInt64Dtype(),
                         pa.bool_(): pd.BooleanDtype(),
                         pa.string(): pd.StringDtype()}
                         #pa.float32(): pd.Float32Dtype(),
                         #pa.float64(): pd.Float64Dtype(),

        to_pandas_kwargs = {'types_mapper':dtype_mapping.get,#}#
                            'split_blocks':True,
                            'self_destruct':True}

        df = tables[0].to_pandas(**to_pandas_kwargs)
        
        if len(tables) > 1:
            for table in tables[1:]:
                df = df.append(table.to_pandas(**to_pandas_kwargs))
                                               
                                               
        
        return df

    def _concat_tables_pyarrow(self, tables):
        """
        Concatenating tables with different dtypes for 'value', using a single dtype.
        PyArrow version.

        ToDo.
        """
        
        pass
