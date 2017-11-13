"""Project: Eskapade - A python-based package for data analysis.

Class: DfMerger

Created: 2016/11/08

Description:
    Algorithm to Merges two pandas DataFrames

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


class DfMerger(Link):
    """Merges two pandas dataframes."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        Store the configuration of the link.

        :param str name: name of link
        :param str input_collection1: datastore key of the first pandas.DataFrame to merge
        :param str input_collection2: datastore key of the second pandas.DataFrame to merge
        :param str output_collection: datastore key of the merged output pandas.DataFrame
        :param str how: merge modus. See pandas documentation.
        :param list on: column names. See pandas documentation.
        :param list columns1: column names of the first pandas.DataFrame. Only these columns are included in the merge.
            If not set, use all columns.
        :param list columns2: column names of the second pandas.DataFrame. Only these columns are included in the merge.
            If not set, use all columns.
        :param bool remove_duplicate_cols2: if True duplicate columns will be taken out before the merge (default=True)
        :param kwargs: all other key word arguments are passed on to the pandas merge function.
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'DfMerger'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             input_collection1=None,
                             input_collection2=None,
                             output_collection='',
                             how='inner',
                             on=['record_id'],
                             columns1=[],
                             columns2=[],
                             remove_duplicate_cols2=True)

        # pass on remaining kwargs to pandas reader
        self.kwargs = copy.deepcopy(kwargs)

    def initialize(self):
        """Perform basic checks on provided attributes."""
        if isinstance(self.on, str):
            self.on = [self.on]
        elif not isinstance(self.on, list):
            raise Exception('on is not a list of strings.')

        assert len(self.on) > 0, 'Not specified on which keys to merge.'
        assert (self.how == 'inner' or self.how == 'outer' or self.how == 'left' or self.how == 'right'), \
            'How to merge not specified correctly.'
        assert len(self.output_collection), 'output_collection not specified.'

        # add back on to kwargs, so it's picked up by pandas.
        if self.on is not None:
            self.kwargs['on'] = self.on
        if self.how is not None:
            self.kwargs['how'] = self.how

        self.logger.info('kwargs passed on to pandas merge function are: {kwargs}.', kwargs=self.kwargs)

        return StatusCode.Success

    def execute(self):
        """Perform merging of input dataframes."""
        ds = process_manager.service(DataStore)

        # Perform basic checks on input collections and provided attributes.

        # datasets okay?
        assert self.input_collection1 in ds, 'Key {} not in DataStore.'.format(self.input_collection1)
        assert self.input_collection2 in ds, 'Key {} not in DataStore.'.format(self.input_collection2)
        df1 = ds[self.input_collection1]
        df2 = ds[self.input_collection2]
        assert isinstance(df1, pd.DataFrame), 'Item {} is not a DataFrame.'.format(self.input_collection1)
        assert isinstance(df2, pd.DataFrame), 'Item {} is not a DataFrame.'.format(self.input_collection2)

        # check provided columns
        if len(self.columns1) == 0:
            self.columns1 = df1.columns
        for col in self.columns1:
            assert col in df1.columns, 'Column <{}> not in input_collection1'.format(col)
        if len(self.columns2) == 0:
            self.columns2 = df2.columns
        for col in self.columns2:
            assert col in df2.columns, 'Column <{}> not in input_collection2'.format(col)
        for col in self.on:
            assert col in self.columns1, 'Key <{}> not in selected columns input_collection1.'.format(col)
            assert col in self.columns2, 'Key <{}> not in selected columns input_collection2.'.format(col)
        if self.remove_duplicate_cols2:
            self.columns2 = [c for c in self.columns2 if c not in self.columns1]
            self.columns2 += self.on

        # then do the merging.
        dfout = pd.merge(df1[self.columns1], df2[self.columns2], **self.kwargs)

        # storage
        ds[self.output_collection] = dfout
        ds['n_' + self.output_collection] = len(dfout.index)

        self.logger.info('Put dataframe <{df}> with length <{length:d}> in data store.',
                         df=self.output_collection, length=len(dfout.index))

        return StatusCode.Success
