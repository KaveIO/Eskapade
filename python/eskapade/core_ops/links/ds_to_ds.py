"""Project: Eskapade - A python-based package for data analysis.

Class: DsToDs

Created: 2016/11/08

Description:
    Algorithm to move, copy, or remove an object in the datastore.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import copy

import pandas as pd

from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class DsToDs(Link):
    """Link to move, copy, or remove an object in the datastore."""

    def __init__(self, **kwargs):
        """
        Initialize link instance.

        :param str name: name of link
        :param str read_key: key of data to read from data store
        :param str store_key: key of data to store in data store
        :param bool move: move read_key item to store_key. Default is true.
        :param bool copy: if True the read_key key, value pair will not be deleted. Default is false.
        :param bool remove: if True the item corresponding to read_key key will be deleted. Default is false.
        :param dict columnsToAdd: if the object is a pandas.DataFrame columns to add to the pandas.DataFrame.
            key = column name, value = column
        """
        Link.__init__(self, kwargs.pop('name', 'DsToDs'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', store_key='', columnsToAdd=None, move=True, copy=False, remove=False)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        assert isinstance(self.read_key, str) and self.read_key, 'read_key not set.'
        if not self.remove:
            assert isinstance(self.store_key, str) and self.store_key, 'store_key not set.'

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        if self.read_key not in ds:
            self.logger.warning('read_key <{key}> not in DataStore. Return.', key=self.read_key)
            return StatusCode.Recoverable

        # if the object is a pandas.DataFrame columns can be added to the dataframe
        df = ds[self.read_key]
        if isinstance(df, pd.DataFrame) and self.columnsToAdd is not None:
            for k, v in self.columnsToAdd.items():
                df[k] = v

        # copy, remove, or move item. default is move
        if self.copy:
            ds[self.store_key] = copy.deepcopy(ds[self.read_key])
        elif self.remove:
            ds.pop(self.read_key)
        elif self.move:
            ds[self.store_key] = ds.pop(self.read_key)

        return StatusCode.Success
