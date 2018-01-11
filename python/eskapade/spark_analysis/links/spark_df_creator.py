"""Project: Eskapade - A python-based package for data analysis.

Class: SparkDfCreator

Created: 2017/06/13

Description:
    Create a Spark data frame from generic input data

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import Link, StatusCode, process_manager, DataStore
from eskapade.helpers import process_transform_funcs
from eskapade.spark_analysis import SparkManager, data_conversion


class SparkDfCreator(Link):
    """Link to create a Spark dataframe from generic input data."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of the input data in the data store
        :param str store_key: key of the output data frame in the data store
        :param schema: schema to create data frame if input data have a different format
        :param iterable process_methods: methods to apply sequentially on the produced data frame
        :param dict process_meth_args: positional arguments for process methods
        :param dict process_meth_kwargs: keyword arguments for process methods
        :param bool fail_missing_data: fail execution if data are missing (default is "True")
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'SparkDfCreator'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', store_key=None, schema=None, process_methods=[],
                             process_meth_args={}, process_meth_kwargs={}, fail_missing_data=True)
        self.kwargs = kwargs

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, process_meth_args=dict, process_meth_kwargs=dict)
        self.check_arg_types(allow_none=True, store_key=str)
        self.check_arg_vals('read_key')
        self.fail_missing_data = bool(self.fail_missing_data)
        if not self.store_key:
            self.store_key = self.read_key

        # process post-process methods
        self._process_methods = process_transform_funcs(self.process_methods, self.process_meth_args,
                                                        self.process_meth_kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # get process manager and data store
        ds = process_manager.service(DataStore)

        # fetch data from data store
        if self.read_key not in ds:
            err_msg = 'No input data found in data store with key "{}".'.format(self.read_key)
            if not self.fail_missing_data:
                self.logger.error(err_msg.capitalize())
                return StatusCode.Success
            raise KeyError(err_msg)
        data = ds[self.read_key]

        # create data frame
        spark = process_manager.service(SparkManager).get_session()
        self.logger.debug('Converting data of type "{type}" to a Spark data frame.', type=type(data))
        ds[self.store_key] = data_conversion.create_spark_df(spark, data, schema=self.schema,
                                                             process_methods=self._process_methods, **self.kwargs)

        return StatusCode.Success
