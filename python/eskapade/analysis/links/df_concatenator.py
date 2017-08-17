# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : DataFrameColumnRenamer                                                *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to concatenate multiple pandas datadrames                       *
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

from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class DfConcatenator(Link):
    """
    Concatenates multiple pandas datadrames.
    """

    def __init__(self, **kwargs):
        """
        Store the configuration of link DfConcatenator

        :param str name: name of link
        :param str storeKey: key of data to store in data store
        :param list readKeys: keys of pandas dataframes in the data store
        :param bool ignore_missing_input: Skip missing input datasets. If all missing, store empty dataset. Default is false.
        :param kwargs: all other key word arguments are passed on to pandas concat function.
        """
        
        Link.__init__(self, kwargs.pop('name', 'DfConcatenator'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs, readKeys=[])
        self._process_kwargs(kwargs, storeKey=None)
        self._process_kwargs(kwargs, ignore_missing_input=False)

        # pass on remaining kwargs to pandas reader 
        self.kwargs = copy.deepcopy(kwargs)
        
        return

    def initialize(self):
        """ Initialize DfConcatenator """

        assert len(self.readKeys), 'readKeys have not been set. Error.'
        assert isinstance(self.storeKey, str) and len(self.storeKey), 'storage key not set.'

        self.log().info('kwargs passed on to pandas concat function are: %s' % self.kwargs )

        return StatusCode.Success

    def execute(self):
        """ Execute DfConcatenator

        Perform concatenation of multiple pandas datadrames.
        """

        ds = process_manager.service(DataStore)

        # check if all input dataframes exist. if so configured, skip missing inputs, else raise e.
        data = []
        for c in self.readKeys:
            if c not in ds:
                if self.ignore_missing_input:
                    self.log().warning("<%s> is not a key in the datastore. Configured to skip it." % c)
                    continue
                raise Exception("<%s> is not a key in the datastore" % c)
            data.append(ds[c])

        # concatenate the dataframes
        if len(data):
            df = pd.concat(data, **self.kwargs).reset_index(drop=True)
        elif self.ignore_missing_input:
            self.log().warning("Nothing to concatenate. Configured to return empty dataframe.")
            df = pd.DataFrame()
        else:
            raise Exception("Nothing to concatenate. This is not right. Exit.")

        # store the result
        ds[self.storeKey] = df
        ds['n_'+self.storeKey] = len(df.index)
        
        return StatusCode.Success
