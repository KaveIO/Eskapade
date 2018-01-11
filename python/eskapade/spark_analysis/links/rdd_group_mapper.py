"""Project: Eskapade - A python-based package for data analysis.

Class: RddGroupMapper

Created: 2017/06/20

Description:
    Apply a map function on groups in a Spark RDD

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pyspark

from eskapade import Link, StatusCode, process_manager, DataStore


class RddGroupMapper(Link):
    """Apply a map function on groups in a Spark RDD.

    Group rows of key-value pairs in a Spark RDD by key and apply a custom
    map function on the group values.  By default, the group key and the
    value returned by the map function forms a single row in the output RDD.
    If the "flatten_output_groups" flag is set, the returned value is
    interpreted as an iterable and a row is created for each item.

    Optionally, a map function is applied on the rows of the input RDD, for
    example to create the group key-value pairs.  Similarly, a function may
    be specified to map the key-value pairs resulting from the group map.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of the input data in the data store
        :param str store_key: key of the output data frame in the data store
        :param group_map: map function for group values
        :param input_map: map function for input rows; optional, e.g. to create group key-value pairs
        :param result_map: map function for output group values; optional, e.g. to flatten group key-value pairs
        :param bool flatten_output_groups: create a row for each item in the group output values (default is False)
        :param int num_group_partitions: number of partitions for group map (optional, no repartitioning by default)
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'RddGroupMapper'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', store_key=None, group_map=None, input_map=None, result_map=None,
                             flatten_output_groups=False, num_group_partitions=None)
        self.kwargs = kwargs

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str)
        self.check_arg_types(allow_none=True, store_key=str, num_group_partitions=int)
        self.check_arg_callable('group_map', 'input_map', 'result_map', allow_none=True)
        self.check_arg_vals('read_key', 'group_map')
        self.flatten_output_groups = bool(self.flatten_output_groups)
        if not self.store_key:
            self.store_key = self.read_key

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # get process manager and data store
        ds = process_manager.service(DataStore)

        # fetch data frame from data store
        if self.read_key not in ds:
            raise KeyError('no input data found in data store with key "{}"'.format(self.read_key))
        data = ds[self.read_key]
        if not isinstance(data, pyspark.RDD):
            raise TypeError('expected a Spark RDD for "{0:s}" (got "{1!s}")'.format(self.read_key, type(data)))

        # apply input map
        if self.input_map:
            data = data.map(self.input_map)

        # group data by keys in the data
        data = data.groupByKey(numPartitions=self.num_group_partitions)

        # apply map on group values
        if self.flatten_output_groups:
            data = data.flatMapValues(self.group_map)
        else:
            data = data.mapValues(self.group_map)

        # apply map on result
        if self.result_map:
            data = data.map(self.result_map)

        # store data in data store
        ds[self.store_key] = data

        return StatusCode.Success
