# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : ApplySelectionToDf                                                    *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to apply queries to input dataframe                             *
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
from eskapade import ProcessManager, StatusCode, DataStore, Link


class ApplySelectionToDf(Link):
    """
    Applies queries with sub-selections to a pandas.DataFrame
    """

    def __init__(self, **kwargs):
        """
        Applies queries with sub-selections to a pandas.DataFrame

        Input dataframe is not overwritten, unless instructed to do so in kwargs.

        :param str name: name of link
        :param str readKey: key of data to read from data store
        :param str storeKey: key of data to store in data store. If not set readKey is overwritten.
        :param list querySet: list of strings, query expressions to evaluate in the same order, see pandas documentation
        :param list selectColumns: column names to select after querying
        :param bool continueIfFailure: if True continues with next query after failure (optional)
        :param kwargs: all other key word arguments are passed on to the pandas queries.
        """

        Link.__init__(self, kwargs.pop('name', 'ApplySelectionToDf'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             readKey='',
                             storeKey=None,
                             querySet=[],
                             selectColumns=[],
                             continueIfFailure=False)
        
        # pass on remaining kwargs to pandas query
        self.kwargs = copy.deepcopy(kwargs)

        return

    def initialize(self):
        """ Initialize ApplySelectionToDf

        Perform checks on provided attributes
        """

        # querySet or selectColumns needs to be set.
        assert len(self.readKey) > 0, 'readKey has not been set.'

        if self.storeKey == None:
            self.log().warning('storeKey has not been set, now set to: %s' % self.readKey)
            self.storeKey = self.readKey
            pass
        else:
            assert isinstance(self.storeKey,str) and len(self.storeKey), 'store Key has not been set.'

        if isinstance(self.querySet, str):
            self.querySet = [self.querySet]
        elif not isinstance(self.querySet, list):
            raise Exception('query set is not a list of strings. Exit.')

        if isinstance(self.selectColumns, str):
            self.selectColumns = [self.selectColumns]
        elif not isinstance(self.selectColumns, list):
            raise Exception('column selection is not a list of strings. Exit.')

        assert len(self.querySet) or len(self.selectColumns), 'No selections have been provided.'

        self.log().info('kwargs passed on to pandas query function are: %s' % self.kwargs )
        
        return StatusCode.Success


    def execute(self):
        """ Execute ApplySelectionToDf

        Applies queries or column selection to a pandas DataFrame.
        Input dataframe is not overwritten, unless told to do so in kwargs.

        1. Apply queries, in order of provided query list.
        2. Select columns (if provided). 
        """

        ds = ProcessManager().service(DataStore)
        assert self.readKey in list(ds.keys()), 'Key %s not in DataStore.' % self.readKey
        assert isinstance(ds[self.readKey],pd.DataFrame), 'Object with key %s is not a pandas DataFrame.' % self.readKey

        # 1. apply queries to input dataframe.
        #    input dataframe is not overwritten, unless told to do so in kwargs.
        do_continue = True
        if len(self.querySet):
            # apply first query
            query = self.querySet[0]
            try:
                df = ds[self.readKey].query(query, **self.kwargs)
            except:
                if not self.continueIfFailure:
                    raise ValueError('Failed to apply query <%s> to dataframe <%s>.' % (query, self.readKey))
                else:
                    orig_df_cols = (ds[self.readKey]).columns
                    df = pd.DataFrame(columns=orig_df_cols)
                    do_continue = False
            # apply rest of the queries if any
            if do_continue:
                for query in self.querySet[1:]:
                    try:
                        df = df.query(query, **self.kwargs)
                    except:
                        if not self.continueIfFailure:
                            raise ValueError('Failed to apply query <%s> to dataframe <%s>.' % (query, self.readKey))
                        else:
                            orig_df_cols = (ds[self.readKey]).columns
                            df = pd.DataFrame(columns=orig_df_cols)
                            break

        # 2. apply column selection to input dataframe.
        #    input dataframe is not overwritten.
        if len(self.selectColumns):
            if not 'df' in vars():
                df = (ds[self.readKey]).copy(deep=False)
            try:
                df = df[self.selectColumns]
            except:
                if not self.continueIfFailure:
                    raise ValueError('Failed to select columns <%s> of dataframe <%s>.' % (str(self.selectColumns),
                                                                                           self.readKey))
                else:
                    df = pd.DataFrame(columns=self.selectColumns)

        assert 'df' in vars(), 'No dataframe available for storage?'

        ds[self.storeKey] = df
        ds['n_'+self.storeKey] = len(df.index)

        self.log().info('Stored dataframe with key <%s> and length <%d>.' % (self.storeKey, len(df.index)))

        return StatusCode.Success
