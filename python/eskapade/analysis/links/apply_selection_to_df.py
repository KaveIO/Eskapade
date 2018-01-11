"""Project: Eskapade - A python-based package for data analysis.

Class: ApplySelectionToDf

Created: 2016/11/08

Description:
    Algorithm to apply queries to input dataframe

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import copy

import pandas as pd

from eskapade import process_manager, DataStore, Link, StatusCode


class ApplySelectionToDf(Link):
    """Applies queries with sub-selections to a pandas dataframe."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        Input dataframe is not overwritten, unless instructed to do so in kwargs.

        :param str name: name of link
        :param str read_key: key of data to read from data store
        :param str store_key: key of data to store in data store. If not set read_key is overwritten.
        :param list query_set: list of strings, query expressions to evaluate in the same order,
            see pandas documentation
        :param list select_columns: column names to select after querying
        :param bool continue_if_failure: if True continues with next query after failure (optional)
        :param kwargs: all other key word arguments are passed on to the pandas queries.
        """
        Link.__init__(self, kwargs.pop('name', 'ApplySelectionToDf'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             store_key=None,
                             query_set=[],
                             select_columns=[],
                             continue_if_failure=False)

        # pass on remaining kwargs to pandas query
        self.kwargs = copy.deepcopy(kwargs)

    def initialize(self):
        """Initialize the link.

        Perform checks on provided attributes.
        """
        # query_set or select_columns needs to be set.
        assert len(self.read_key) > 0, 'read_key has not been set.'

        if self.store_key is None:
            self.logger.warning('store_key has not been set, now set to: {key}', key=self.read_key)
            self.store_key = self.read_key
        else:
            assert isinstance(self.store_key, str) and len(self.store_key), 'store_key has not been set.'

        if isinstance(self.query_set, str):
            self.query_set = [self.query_set]
        elif not isinstance(self.query_set, list):
            raise Exception('Query set is not a list of strings.')

        if isinstance(self.select_columns, str):
            self.select_columns = [self.select_columns]
        elif not isinstance(self.select_columns, list):
            raise Exception('Column selection is not a list of strings.')

        assert len(self.query_set) or len(self.select_columns), 'No selections have been provided.'

        self.logger.info('kwargs passed on to pandas query function are: {kwargs}', kwargs=self.kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Applies queries or column selection to a pandas DataFrame.
        Input dataframe is not overwritten, unless told to do so in kwargs.

        1. Apply queries, in order of provided query list.
        2. Select columns (if provided).
        """
        ds = process_manager.service(DataStore)
        assert self.read_key in ds, 'Key "{key}" not in DataStore.'.format(key=self.read_key)
        assert isinstance(ds[self.read_key],
                          pd.DataFrame), 'Object with key {} is not a pandas DataFrame.'.format(self.read_key)

        # 1. apply queries to input dataframe.
        #    input dataframe is not overwritten, unless told to do so in kwargs.
        do_continue = True
        if len(self.query_set):
            # apply first query
            query = self.query_set[0]
            try:
                df = ds[self.read_key].query(query, **self.kwargs)
            except Exception:
                if not self.continue_if_failure:
                    raise ValueError('Failed to apply query <{}> to dataframe <{}>.'.format(query, self.read_key))
                else:
                    orig_df_cols = (ds[self.read_key]).columns
                    df = pd.DataFrame(columns=orig_df_cols)
                    do_continue = False
            # apply rest of the queries if any
            if do_continue:
                for query in self.query_set[1:]:
                    try:
                        df = df.query(query, **self.kwargs)
                    except Exception:
                        if not self.continue_if_failure:
                            raise ValueError(
                                'Failed to apply query <{}> to dataframe <{}>.'.format(query, self.read_key))
                        else:
                            orig_df_cols = (ds[self.read_key]).columns
                            df = pd.DataFrame(columns=orig_df_cols)
                            break

        # 2. apply column selection to input dataframe.
        #    input dataframe is not overwritten.
        if len(self.select_columns):
            if 'df' not in vars():
                df = (ds[self.read_key]).copy(deep=False)
            try:
                df = df[self.select_columns]
            except Exception:
                if not self.continue_if_failure:
                    raise ValueError(
                        'Failed to select columns <{self.select_columns!s}> of dataframe <{self.read_key}>.'
                        .format(self=self))
                else:
                    df = pd.DataFrame(columns=self.select_columns)

        assert 'df' in vars(), 'No dataframe available for storage?'

        ds[self.store_key] = df
        ds['n_' + self.store_key] = len(df.index)

        self.logger.info('Stored dataframe with key <{key}> and length <{length:d}>.',
                         key=self.store_key, length=len(df.index))

        return StatusCode.Success
