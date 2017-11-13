"""Project: Eskapade - A python-based package for data analysis.

Class: SparkDfConverter

Created: 2017/06/15

Description:
    Convert a Spark data frame into data of a different format

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pyspark

from eskapade import Link, StatusCode, process_manager, DataStore
from eskapade.helpers import apply_transform_funcs, process_transform_funcs

OUTPUT_FORMATS = ('df', 'rdd', 'list', 'pd')


class SparkDfConverter(Link):
    """Link to convert a Spark data frame into a different format.

    A data frame from the data store is converted into data of a different
    format and/or transformed. The format conversion is controlled by the
    "output_format" argument.  The data frame can either be unchanged ("df",
    default) or converted into a Spark RDD of tuples ("RDD"), a list of
    tuples ("list"), or a Pandas data frame ("pd").

    After the format conversion, the data can be transformed by functions
    specified by the "process_methods" argument.  These functions will be
    sequentially applied to the output of the previous function.  Each
    function is specified by either a callable object or a string.  A string
    will be interpreted as the name of an attribute of the dataset type.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of the input data in the data store
        :param str store_key: key of the output data frame in the data store
        :param str schema_key: key to store the data-frame schema in the data store
        :param str output_format: data format to store: {"df" (default), "RDD", "list", "pd"}
        :param bool preserve_col_names: preserve column names for non-data-frame output formats (default is True)
        :param iterable process_methods: methods to apply sequentially on the produced data
        :param dict process_meth_args: positional arguments for process methods
        :param dict process_meth_kwargs: keyword arguments for process methods
        :param bool fail_missing_data: fail execution if the input data frame is missing (default is "True")
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'SparkDfConverter'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', store_key=None, schema_key=None, output_format='df',
                             preserve_col_names=True, process_methods=[], process_meth_args={}, process_meth_kwargs={},
                             fail_missing_data=True)
        self.kwargs = kwargs

    def initialize(self):
        """Initialize SparkDfConverter."""
        # check input arguments
        self.check_arg_types(read_key=str, output_format=str, process_meth_args=dict, process_meth_kwargs=dict)
        self.check_arg_types(allow_none=True, store_key=str, schema_key=str)
        self.check_arg_vals('read_key')
        self.preserve_col_names = bool(self.preserve_col_names)
        self.fail_missing_data = bool(self.fail_missing_data)
        if not self.store_key:
            self.store_key = self.read_key
        if not self.schema_key:
            self.schema_key = '{}_schema'.format(self.store_key)

        # check output format
        self.output_format = self.output_format.lower()
        if self.output_format not in OUTPUT_FORMATS:
            self.logger.fatal('Specified data output format "{format}" is invalid.', format=self.output_format)
            raise RuntimeError('Invalid output format specified.')

        # set process methods
        self._process_methods = process_transform_funcs(self.process_methods, self.process_meth_args,
                                                        self.process_meth_kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # get process manager and data store
        ds = process_manager.service(DataStore)

        # fetch data frame from data store
        if self.read_key not in ds:
            err_msg = 'No input data found in data store with key "{}".'.format(self.read_key)
            if not self.fail_missing_data:
                self.logger.error(err_msg.capitalize())
                return StatusCode.Success
            raise KeyError(err_msg)
        data = ds[self.read_key]
        if not isinstance(data, pyspark.sql.DataFrame):
            raise TypeError('Expected a Spark data frame for "{0:s}" (got "{1!s}").'.format(self.read_key, type(data)))

        # store data-frame schema
        ds[self.schema_key] = data.schema

        # convert data frame
        if self.output_format == 'rdd':
            # convert to RDD
            data = data.rdd
            if not self.preserve_col_names:
                # convert rows to tuples, which removes column names
                data = data.map(tuple)
        elif self.output_format == 'list':
            # convert to a list
            data = data.collect()
            if not self.preserve_col_names:
                # convert rows to tuples, which removes column names
                data = list(map(tuple, data))
        elif self.output_format == 'pd':
            # convert to Pandas data frame
            data = data.toPandas()

        # further process created dataset
        data = apply_transform_funcs(data, self._process_methods)

        # store data in data store
        ds[self.store_key] = data

        return StatusCode.Success
