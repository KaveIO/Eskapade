"""Project: Eskapade - A python-based package for data analysis.

Class: SparkDfReader

Created: 2016/11/08

Description:
    Read data into a Spark data frame

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import Link, StatusCode, process_manager, DataStore
from eskapade.helpers import apply_transform_funcs, process_transform_funcs
from eskapade.spark_analysis import SparkManager


class SparkDfReader(Link):
    """Link to read data into a Spark dataframe.

    Data are read with the Spark-SQL data-frame reader
    (pyspark.sql.DataFrameReader).  The read-method to be applied on the
    reader instance (load, parquet, csv, ...) can be specified by the user,
    including its arguments.  In addition to the read method, also other
    functions to be applied on the reader (schema, option, ...) and/or the
    resulting data frame (filter, select, repartition, ...) can be included.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str store_key: key of data to store in data store
        :param iterable read_methods: methods to apply sequentially on data-frame reader and data frame
        :param dict read_meth_args: positional arguments for read methods
        :param dict read_meth_kwargs: keyword arguments for read methods
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'SparkDfReader'))

        # process keyword arguments
        self._process_kwargs(kwargs, store_key='', read_methods=[], read_meth_args={}, read_meth_kwargs={})
        self.check_extra_kwargs(kwargs)

        # initialize other attributes
        self.schema = None

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(store_key=str, read_meth_args=dict, read_meth_kwargs=dict)
        self.check_arg_vals('store_key', 'read_methods')

        # process data-frame-reader methods
        self._read_methods = process_transform_funcs(self.read_methods, self.read_meth_args, self.read_meth_kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # create data-frame reader
        spark = process_manager.service(SparkManager).get_session()
        data = spark.read

        # call data-frame reader methods
        data = apply_transform_funcs(data, self._read_methods)

        # store data in data store
        process_manager.service(DataStore)[self.store_key] = data

        return StatusCode.Success
