"""Project: Eskapade - A python-based package for data analysis.

Class: DataFrameColumnRenamer

Created: 2016/11/08

Description:
    Algorithm to concatenate multiple pandas datadrames

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


class DfConcatenator(Link):
    """Concatenates multiple pandas datadrames."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str store_key: key of data to store in data store
        :param list read_keys: keys of pandas dataframes in the data store
        :param bool ignore_missing_input: Skip missing input datasets.
            If all missing, store empty dataset. Default is false.
        :param kwargs: all other key word arguments are passed on to pandas concat function.
        """
        Link.__init__(self, kwargs.pop('name', 'DfConcatenator'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs, read_keys=[])
        self._process_kwargs(kwargs, store_key=None)
        self._process_kwargs(kwargs, ignore_missing_input=False)

        # pass on remaining kwargs to pandas reader
        self.kwargs = copy.deepcopy(kwargs)

    def initialize(self):
        """Initialize the link."""
        assert self.read_keys, 'read_keys have not been set.'
        assert isinstance(self.store_key, str) and self.store_key, 'storage key not set.'

        self.logger.info('kwargs passed on to pandas concat function are: {kwargs}', kwargs=self.kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Perform concatenation of multiple pandas datadrames.
        """
        ds = process_manager.service(DataStore)

        # check if all input dataframes exist. if so configured, skip missing inputs, else raise e.
        data = []
        for c in self.read_keys:
            if c not in ds:
                if self.ignore_missing_input:
                    self.logger.warning("<{key}> is not a key in the datastore. Configured to skip it.", key=c)
                    continue
                raise Exception("<{}> is not a key in the datastore.".format(c))
            data.append(ds[c])

        # concatenate the dataframes
        if len(data):
            df = pd.concat(data, **self.kwargs).reset_index(drop=True)
        elif self.ignore_missing_input:
            self.logger.warning("Nothing to concatenate. Configured to return empty dataframe.")
            df = pd.DataFrame()
        else:
            raise Exception("Nothing to concatenate.")

        # store the result
        ds[self.store_key] = df
        ds['n_' + self.store_key] = len(df.index)

        return StatusCode.Success
