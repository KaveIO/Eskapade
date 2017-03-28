# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : RecordFactorizer                                                      *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to perform the factorization of an input column
# *      of an input dataframe.
# *      E.g. a columnn x with values 'apple', 'tree', 'pear', 'apple', 'pear'
# *      is tranformed into columns x with values 0, 1, 2, 0, 2, etc.
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import pandas as pd
from pandas import DataFrame
from functools import reduce

from eskapade import ProcessManager, Link, StatusCode, DataStore


class RecordFactorizer(Link):
    """Factorize data-frame columns

    Perform factorization of input column of an input dataframe.  E.g. a
    columnn x with values 'apple', 'tree', 'pear', 'apple', 'pear' is
    tranformed into columns x with values 0, 1, 2, 0, 2, etc.
    """

    def __init__(self, **kwargs):
        """Initialize RecordFactorizer instance

        Store and do basic check on the attributes of link RecordFactorizer

        :param str read_key: key to read dataframe from the data store. Dataframe of records that is to be transformed.
        :param str store_key: store key of output dataFrame. Default is read_key + '_factorized'. (optional)
        :param list columns: list of columns that are to be factorized
        :param bool inplace: replace oritinal columns. Default is False. Overwrites store_key to read_key.
        """

        Link.__init__(self, kwargs.pop('name', 'RecordFactorizer'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             store_key=None,
                             columns=[],
                             inplace=False)

        # check residual kwargs. exit if any present
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize RecordFactorizer

        Initialize and (further) check the assigned attributes of
        the RecordFactorizer
        """

        self.check_arg_types(read_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str)
        self.check_arg_vals('read_key')

        if self.inplace:
            self.store_key = self.read_key
            self.log().info('store_key has been set to read_key "%s"', self.store_key)

        if self.store_key is None:
            self.store_key = self.read_key + '_factorized'
            self.log().info('store_key was empty, has been set to "%s"', self.store_key)

        return StatusCode.Success

    def execute(self):
        """Execute RecordFactorizer

        Perform factorization input column 'column' of input dataframe.
        Resulting dataset stored as new dataset.
        """

        ds = ProcessManager().service(DataStore)

        # basic checks on contensts of the data frame
        if self.read_key not in ds:
            raise KeyError('Key "%s" not in DataStore' % self.read_key)
        df = ds[self.read_key]
        if not isinstance(df, DataFrame):
            raise TypeError('retrieved object not of type pandas DataFrame')
        if len(df.index) == 0:
            raise AssertionError('dataframe "%s" is empty' % self.read_key)
        for c in self.columns:
            if c not in df.columns:
                raise AssertionError('column "%s" not present in input data frame' % c)

        # do factorization for all columns
        mapping = {}
        df_fact = df if self.inplace else pd.DataFrame(index=df.index)
        for c in self.columns:
            labels, unique = df[c].factorize()
            df_fact[c] = labels
            mapping[c] = unique

        ds[self.store_key] = df_fact
        ds['map_' + self.store_key] = mapping

        return StatusCode.Success
