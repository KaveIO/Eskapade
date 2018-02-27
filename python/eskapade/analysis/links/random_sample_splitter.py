"""Project: Eskapade - A python-based package for data analysis.

Class: RandomSampleSplitter

Created: 2016/11/08

Description:
    Algorithm to randomly assign records to a number of classes

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pandas as pd
from numpy.random import RandomState

from eskapade import process_manager, ConfigObject, DataStore, Link, StatusCode


class RandomSampleSplitter(Link):
    """Link that randomly assigns records of an input dataframe to a number of classes.

    After assigning classes does one of the following:

    - splits the input dataframe into sub dataframes according classes and stores the sub dataframes into the datastore;
    - add a new column with assigned classes to the dataframe.

    Records are assigned randomly.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of data to read from datastore
        :param list store_key: keys of datasets to store in datastore.
            Number of sub samples equals length of store_key list (optional instead of 'column' and 'nclasses').
        :param str column: name of new column that specifies the randomly assigned class.
            Default is randomclass (optional instead of 'store_key').
        :param int nclasses: number of random classes. Needs to be set
            (optional instead of 'store_key').
        :param list fractions: list of fractions (0<fraction<1) of records assigned to the sub samples.
            Can be one less than n classes. Sum can be less than 1. Needs to be set.
        :param list nevents: list of number of random records assigned to the sub samples
            Can be one less than n classes (optional instead of 'fractions').
        """
        Link.__init__(self, kwargs.pop('name', 'RandomSampleSplitter'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key=None,
                             store_key=None,
                             column='randomclass',
                             fractions=None,
                             nevents=False,
                             nclasses=None)

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def _convert_attr_to_list(self, var_name):
        """Convert a float attribute to a list.

        If the attribute is a single float, converts it to a list.
        If the attribute is neither list nor float, throws an exception.
        :param var_name: Attribute name.
        """
        var = getattr(self, var_name)
        if isinstance(var, float):
            setattr(self, var_name, [var])
        elif not isinstance(var, list):
            raise Exception('Given {var_name} of incorrect type.'.format(var_name=var_name))

    def initialize(self):
        """Check and initialize attributes of the link."""
        # check that these four attributes have been set correctly.
        assert isinstance(self.read_key, str) and self.read_key, 'read key not set.'
        # either store keys or nclasses and column need to be specified.
        if self.store_key is not None:
            if isinstance(self.store_key, str) and self.store_key:
                self.store_key = [self.store_key]
            elif not (isinstance(self.store_key, list) and len(self.store_key)):
                raise Exception('Store keys not filled with list of strings.')
            self.nclasses = len(self.store_key)
        else:
            assert isinstance(self.column, str) and self.column, 'Column not set.'
            assert isinstance(self.nclasses, int) and self.nclasses > 1, \
                'Number of random classes need to be int and greater than 1.'

        assert isinstance(self.nclasses, int) and self.nclasses >= 1, \
            'number of random classes need to be int and greater than or equal to 1'
        if self.fractions is None and self.nevents is None:
            raise Exception('No fractions or nevents are provided. Provide at least one.')

        # check that provided fractions are all okay.
        if self.fractions is not None:
            self._convert_attr_to_list('fractions')

            if len(self.fractions) == self.nclasses - 1:
                sumf = sum(f for f in self.fractions)
                self.fractions.append(1. - sumf)
            assert (len(self.fractions) == self.nclasses), \
                'Number of fractions <{:d}> doesnt equal number of classes <{:d}>.'.format(
                    len(self.fractions), self.nclasses)
            for i, f in enumerate(self.fractions):
                assert f >= 0, '<{:d}> assigned fraction <{:f}> needs to be greater than zero.'.format(i, f)
            # normalize fractions
            sumf = sum(f for f in self.fractions)
            for i, f in enumerate(self.fractions):
                if sumf > 1:
                    self.fractions[i] = f / sumf
                self.logger.info('Random class <{index:d}> assigned fraction is <{fraction:f}>.',
                                 index=i, fraction=self.fractions[i])

        # alternatively, check that provided number of events per random class are all okay.
        if self.nevents is not None:
            self._convert_attr_to_list('nevents')
            if not ((len(self.nevents) == self.nclasses) or (len(self.nevents) == self.nclasses - 1)):
                raise Exception('number of provided events <{:d}> does not equal number of classes <{:d}>.'
                                .format(len(self.nevents), self.nclasses))

        # there needs to be a random seed set in the configobject
        settings = process_manager.service(ConfigObject)
        assert 'seed' in settings, 'random seed not set in ConfigObject.'
        self._seed = settings['seed']

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        # basic checks on contensts of the data frame
        assert self.read_key in ds, 'Key "{key}" not in DataStore.'.format(key=self.read_key)
        df = ds[self.read_key]
        if not isinstance(df, pd.DataFrame):
            raise Exception('Retrieved object not of type pandas DataFrame.')
        ndf = len(df.index)
        assert ndf > 0, 'dataframe {} is empty.'.format(self.read_key)
        if self.store_key is None:
            if self.column in df.columns:
                raise Exception('Column name <{}> already used: <{!s}>. Will not overwrite.'
                                .format(self.column, df.columns))
            df[self.column] = 0

        # fix final number of events assigned per random class
        # ... each class gets at least one event
        if self.nevents is not None:
            if len(self.nevents) == self.nclasses - 1:
                self.nevents.append(ndf - sum(n for n in self.nevents))
        else:
            self.nevents = [int(ndf * f) for f in self.fractions]
        for i in range(self.nclasses):
            nsum = sum(n for n in self.nevents[:i + 1])
            ndiff = 0 if nsum - ndf < 0 else nsum - ndf
            self.nevents[i] -= ndiff
            if self.nevents[i] < 0:
                self.nevents[i] = 0
            self.logger.info('Random class <{index:d}> assigned <{n:d}> events.', index=i, n=self.nevents[i])

        # random reshuffling of dataframe indices
        RNG = RandomState(self._seed)
        permute = RNG.permutation(df.index)

        # apply the random reshuffling, and assign records to the n datasets
        for i in range(self.nclasses):
            ib = sum(n for n in self.nevents[:i])
            ie = sum(n for n in self.nevents[:i + 1])
            if self.store_key is None:
                df.ix[permute[ib:ie], self.column] = i
            else:
                ds[self.store_key[i]] = df.ix[permute[ib:ie]]
                self.logger.info('Stored output collection <{key}> with <{n:d}> records in datastore.',
                                 key=self.store_key[i], n=len(ds[self.store_key[i]].index))

        # increase seed in case of next iteration
        self._seed += 1

        return StatusCode.Success
