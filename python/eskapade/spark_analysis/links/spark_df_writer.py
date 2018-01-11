"""Project: Eskapade - A python-based package for data analysis.

Class: SparkDfWriter

Created: 2016/11/08

Description:
    Write data from a Spark data frame

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pyspark

from eskapade import Link, StatusCode, process_manager, DataStore
from eskapade.helpers import apply_transform_funcs, process_transform_funcs
from eskapade.spark_analysis import SparkManager, data_conversion


class SparkDfWriter(Link):
    """Link to write data from a Spark dataframe.

    Data are written with the Spark-SQL data-frame writer
    (pyspark.sql.DataFrameWriter).  The write method to be applied (save,
    parquet, csv, ...) can be specified by the user, including its
    arguments.  In addition to the write method, also other functions to be
    applied on the writer (format, option, ...) can be included.

    If the input format is not a Spark data frame, an attempt is made to
    convert to a data frame.  This works for lists, Spark RDDs, and Pandas
    data frames.  A schema may be specified for the created data frame.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input data in data store
        :param schema: schema to create data frame if input data have a different format
        :param iterable write_methods: methods to apply sequentially on data-frame writer
        :param dict write_meth_args: positional arguments for write methods
        :param dict write_meth_kwargs: keyword arguments for write methods
        :param int num_files: requested number of output files
        :param bool fail_missing_data: fail execution if data are missing (default is "True")
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'SparkDfWriter'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', schema=None, write_methods=[], write_meth_args={},
                             write_meth_kwargs={}, num_files=1, fail_missing_data=True)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, write_meth_args=dict, write_meth_kwargs=dict)
        self.check_arg_vals('read_key', 'write_methods')
        self.fail_missing_data = bool(self.fail_missing_data)
        if self.num_files < 1:
            raise RuntimeError('Requested number of files is less than 1 ({:d}).'.format(self.num_files))

        # process data-frame-writer methods
        self._write_methods = process_transform_funcs(self.write_methods, self.write_meth_args, self.write_meth_kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # get process manager and data store
        ds = process_manager.service(DataStore)

        # check if data frame exists in data store
        if self.read_key not in ds:
            err_msg = 'No input data found in data store with key "{}".'.format(self.read_key)
            if not self.fail_missing_data:
                self.logger.error(err_msg.capitalize())
                return StatusCode.Success
            raise KeyError(err_msg)

        # fetch data from data store
        data = ds[self.read_key]
        if not isinstance(data, pyspark.sql.DataFrame):
            spark = process_manager.service(SparkManager).get_session()
            self.logger.debug('Converting data of type "{type}" to a Spark data frame.', type=type(data))
            data = data_conversion.create_spark_df(spark, data, schema=self.schema)

        # create data-frame writer with requested number of partitions/output files
        df_writer = data.repartition(self.num_files).write

        # call data-frame writer methods
        apply_transform_funcs(df_writer, self._write_methods)

        return StatusCode.Success
