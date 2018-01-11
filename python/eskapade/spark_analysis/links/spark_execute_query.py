"""Project: Eskapade - A python-based package for data analysis.

Class: SparkExecuteQuery

Created: 2017/11/08

Description:
    SparkExecuteQuery applies a SQL-query to one or more objects in the
    DataStore and adds the output of the query to the DataStore as a
    Spark dataframe, RDD or Pandas dataframe.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade import spark_analysis

OUTPUT_FORMATS = ['df', 'rdd', 'pd']


class SparkExecuteQuery(Link):
    """Defines the content of link SparkExecuteQuery.

    Applies a SQL-query to one or more objects in the DataStore.
    Such SQL-queries can for instance be used to filter Spark
    dataframes. All objects in the DataStore are registered as SQL
    temporary views. The output of the query can be added to the
    DataStore as a Spark dataframe (default), RDD or Pandas dataframe.
    """

    def __init__(self, **kwargs):
        """Store the configuration of link SparkExecuteQuery.

        :param str name: name of link
        :param str store_key: key of data to store in data store
        :param str output_format: data format to store: {"df" (default), "rdd", "pd"}
        :param str query: a string containing a SQL-query.
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'SparkSQL'))

        # process keyword arguments
        self._process_kwargs(kwargs, store_key='', query='', output_format='df')
        self.check_extra_kwargs(kwargs)

        # initialize other attributes
        self.schema = None

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(store_key=str, query=str, output_format=str)
        self.check_arg_vals('store_key', 'query')

        # check output format
        if self.output_format not in OUTPUT_FORMATS:
            self.logger.fatal('Specified data output format "{format:s}" is invalid.', format=self.output_format)
            raise RuntimeError('Invalid output format specified.')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        self.logger.debug('Applying following SQL-query to object(s) in DataStore: {query:s}.', query=self.query)

        ds = process_manager.service(DataStore)

        # register all objects in DataStore as SQL temporary views
        for key in ds:
            spark_df = ds[key]
            spark_df.createOrReplaceTempView(key)

        # get existing SparkSession
        spark = process_manager.service(spark_analysis.SparkManager).get_session()

        # apply SQL-query to temporary view(s)
        result = spark.sql(self.query)

        # store dataframe schema
        self.schema = result.schema

        # convert to different data format if required
        if self.output_format == 'rdd':
            # convert to RDD of tuples
            result = result.rdd.map(tuple)
        elif self.output_format == 'pd':
            # convert to Pandas dataframe
            result = result.toPandas()

        ds[self.store_key] = result

        return StatusCode.Success
