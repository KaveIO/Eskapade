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
    Resulting dataset stored as new dataset.
    Alternatively, map transformed columns back to orginal format.
    """

    def __init__(self, **kwargs):
        """Initialize RecordFactorizer instance

        Store and do basic check on the attributes of link RecordFactorizer

        :param str read_key: key to read dataframe from the data store. Dataframe of records that is to be transformed.
        :param str store_key: store key of output dataFrame. Default is read_key + '_fact'. (optional)
        :param str store_key_map: store key of dictionary to map back factorized columns.
               Default is 'map' + '_' + store_key + '_to_original'. (optional)
        :param list columns: list of columns that are to be factorized
        :param dict map_back: dictiorary or key do dictionary to map back factorized columns to original. 
               map_back is a dict of dicts, one dict for each column.
        :param bool inplace: replace oritinal columns. Default is False. Overwrites store_key to read_key.
        """

        Link.__init__(self, kwargs.pop('name', 'RecordFactorizer'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             store_key='',
                             store_key_map='',
                             columns=[],
                             map_back={},
                             inplace=False)

        # check residual kwargs. exit if any present
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize RecordFactorizer

        Initialize and (further) check the assigned attributes of
        the RecordFactorizer
        """

        self.check_arg_types(read_key=str, store_key=str, store_key_map=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str)
        self.check_arg_vals('read_key')

        if self.inplace:
            self.store_key = self.read_key
            self.log().info('store_key has been set to read_key "%s"', self.store_key)

        if not self.store_key:
            self.store_key = self.read_key + '_fact'
            self.log().info('store_key has been set to "%s"', self.store_key)
        if not self.store_key_map:
            self.store_key_map = 'map_' + self.store_key + '_to_original'
            self.log().info('store_key_map has been set to "%s"', self.store_key_map)

        if not self.map_back:
            assert isinstance(self.map_back,str) or isinstance(self.map_back,dict), \
                'map_back needs to be a dict or string (to fetch a dict from the datastore)'

        return StatusCode.Success

    def execute(self):
        """Execute RecordFactorizer

        Perform factorization input columns 'columns' of input dataframe.
        Resulting dataset stored as new dataset.
        Alternatively, map transformed columns back to orginal format.
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
        # retrieve map_back from ds 
        if self.map_back:
            if isinstance(self.map_back,str):
                assert len(self.map_back), 'map_back needs to be a filled string.'
                assert self.map_back in ds, 'map_back key <%s> not found in datastore.'
                self.map_back = ds[self.map_back]
            assert isinstance(self.map_back,dict), 'map_back needs to be a dict'
            
        # 1. do factorization for all specified columns
        if not self.map_back:
            df_fact = df if self.inplace else pd.DataFrame(index=df.index)
            for c in self.columns:
                self.log().debug('Factorizing column <%s> of dataframe <%s>' % (c, self.read_key))
                labels, unique = df[c].factorize()
                df_fact[c] = labels
                self.map_back[c] = dict((i,v) for i,v in enumerate(unique))
            # store the mapping here
            ds[self.store_key_map] = self.map_back
        # 2. do the mapping back to original format
        else:
            self.log().debug('ReFactorizing columns %s of dataframe <%s>' % \
                             (list(self.map_back.keys()), self.read_key))
            df_fact = df.replace(self.map_back, inplace=self.inplace)
            if self.inplace:
                df_fact = df

        # storage
        ds[self.store_key] = df_fact

        return StatusCode.Success
