# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : DsToDs                                                                *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to move, copy, or remove an object in the datastore.            *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import copy

import pandas as pd

from eskapade import process_manager
from eskapade import StatusCode
from eskapade import DataStore
from eskapade import Link


class DsToDs(Link):
    """
    Moves or copies an object in the datastore.
    """

    def __init__(self, **kwargs):
        """
        Link to move, copy, or remove an object in the datastore.

        :param str name: name of link
        :param str readKey: key of data to read from data store
        :param str storeKey: key of data to store in data store
        :param bool move: move readKey item to storeKey. Default is true.
        :param bool copy: if True the readKey key, value pair will not be deleted. Default is false.
        :param bool remove: if True the item corresponding to readKey key will be deleted. Default is false.
        :param dict columnsToAdd: if the object is a pandas.DataFrame columns to add to the pandas.DataFrame.
            key = column name, value = column
        """

        Link.__init__(self, kwargs.pop('name', 'DsToDs'))

        # process keyword arguments
        self._process_kwargs(kwargs, readKey='', storeKey='', columnsToAdd=None, move=True, copy=False, remove=False)
        self.check_extra_kwargs(kwargs)

        return

    def initialize(self):
        """ Initialize DsToDs """

        assert isinstance(self.readKey, str) and len(self.readKey) > 0, 'read key not set.'
        if not self.remove:
            assert isinstance(self.storeKey, str) and len(self.storeKey) > 0, 'store key not set.'

        return StatusCode.Success

    def execute(self):
        """ Execute DsToDs """

        ds = process_manager.service(DataStore)

        if self.readKey not in ds:
            self.log().warning('read key <%s> not in DataStore. Return.' % self.readKey)
            return StatusCode.Recoverable

        # if the object is a pandas.DataFrame columns can be added to the dataframe
        df = ds[self.readKey]
        if isinstance(df, pd.DataFrame) and self.columnsToAdd is not None:
            for k, v in self.columnsToAdd.items():
                df[k] = v

        # copy, remove, or move item. default is move
        if self.copy:
            ds[self.storeKey] = copy.deepcopy(ds[self.readKey])
        elif self.remove:
            ds.pop(self.readKey)
        elif self.move:
            ds[self.storeKey] = ds.pop(self.readKey)

        return StatusCode.Success
