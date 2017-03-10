# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : RecordVectorizer                                                      *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to perform the vectorization of an input column
# *      of an input dataframe.
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

from eskapade import ProcessManager, Link, StatusCode, DataStore


class RecordVectorizer(Link):
    """
    Perform vectorization of input column of an input dataframe.

    E.g. columnn x with values 1, 2 is tranformed into columns x_1 and x_2, with 
    values True or False assigned per record.
    """

    def __init__(self, **kwargs):
        """
        Store and do basic check on the attributes of link RecordVectorizer

        :param str readKey: key to read dataframe from the data store. Dataframe of records that is to be transformed.
        :param str column: column that is to be vectorized
        :param str storeKey: store key of output dataFrame. Default is readKey + '_vectorized'. (optional.)
        :param list column_compare_set: list of unique items with which column values are compared. If not given, this is derived automatically from the column. (optional.)
        """

        Link.__init__(self, kwargs.pop('name', 'RecordVectorizer'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             readKey='',
                             storeKey=None,
                             column='',
                             column_compare_set=None)
        
        # check residual kwargs. exit if any present. 
        self.check_extra_kwargs(kwargs)
        
        return

    def initialize(self):
        """ Initialize and (further) check the assigned attributes of RecordVectorizer """

        assert self.readKey != '', 'DataStore read key has not been set!'
        if self.storeKey is None:
            self.storeKey = self.readKey + '_vectorized'
            self.log().info('store key was empty, has been set to <%s>' % self.storeKey)
        
        return StatusCode.Success


    def execute(self):
        """ Execute RecordVectorizer 

        Perform vectorization input column 'column' of input dataframe.
        Resulting dataset stored as new dataset.
        """

        ds = ProcessManager().service(DataStore)

        # basic checks on contensts of the data frame
        assert self.readKey in list(ds.keys()), 'Key %s not in DataStore.' % self.readKey
        df = ds[self.readKey]
        if not isinstance(df, DataFrame):
            raise Exception('Retrieved object not of type pandas DataFrame.')
        ndf = len(df.index)
        assert ndf > 0, 'dataframe %s is empty.' % self.readKey
        assert self.column in df.columns, 'Column name <%s> not present in input data frame.' % (self.column)

        # checks of column_compare_set
        if self.column_compare_set is None:
            self.column_compare_set = df[self.column].unique()
        elif isinstance(self.column_compare_set,str) and len(self.column_compare_set):
            assert self.column_compare_set in ds, 'Column compare set <%s> not found in data store.' % \
                (self.column_compare_set)
            self.column_compare_set = df[self.column_compare_set]
        if not isinstance(self.column_compare_set,list):
            raise RuntimeError('Column compare set not set correctly.')

        # do vectorization
        ds[self.storeKey] = record_vectorizer(df, self.column, self.column_compare_set)

        return StatusCode.Success


def record_vectorizer(df, column_to_vectorize, column_compare_set):
    """
    Takes the new record that is already transformed and vectorizes the given columns.

    :param df: dataframe of the new record to vectorize
    :param str column_to_vectorize: string, column in the new record to vectorize.
    :param list new_columns: list of strings, columns to be vectorized in the new record df.
    :return: dataframe of the new records.
    """

    dfNew = pd.DataFrame(index=df.index)
    for col in column_compare_set:
        newcol = column_to_vectorize + '_' + str(col)
        dfNew[newcol] = (df[column_to_vectorize] == col).astype(bool)
    return dfNew

