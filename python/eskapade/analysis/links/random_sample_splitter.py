# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : RandomSampleSplitter                                                  *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      RandomSampleSplitter splits an input dataframe into N random sub frames.  *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import pandas as pd
from numpy.random import RandomState

from eskapade import process_manager, ConfigObject, DataStore, Link, StatusCode


class RandomSampleSplitter(Link):

    """Link that splits an input dataframe into a number of sub data-frames.

    Records are assigned randomly.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        Store the configuration of link RandomSampleSplitter.

        :param str name: name of link
        :param str read_key: key of data to read from data store
        :param list store_key: keys of datasets to store in data store.
            Number of sub samples equals length of store_key list.
        :param list fractions: list of fractions (0<fraction<1) of records assigned to the sub samples.
            Sum can be less than 1. Needs to be set.
        :param list nevents: list of number of random records assigned to the sub samples
            (optional instead of 'fractions').
        """
        Link.__init__(self, kwargs.pop('name', 'RandomSampleSplitter'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key=None,
                             store_key=None,
                             fractions=None,
                             nevents=None)
        
        # check residual kwargs. exit if any present. 
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Check and initialize attributes of the link."""
        # check that these four attributes have been set correctly.
        assert isinstance(self.read_key, str) and len(self.read_key), 'read key not set.'
        if isinstance(self.store_key, str) and len(self.store_key):
            self.store_key = [self.store_key]
        elif not (isinstance(self.store_key, list) and len(self.store_key)):
            raise Exception('storage keys not filled with list of strings. Exit.')
        self._nclasses = len(self.store_key)
        assert isinstance(self._nclasses, int) and self._nclasses >= 1, \
            'number of random classes need to be int and greater than or equal to 1'
        if self.fractions is None and self.nevents is None:
            raise Exception('No fractions or nevents are provided. Provide at least one.')

        # check that provided fractions are all okay.
        if self.fractions is not None:
            if isinstance(self.fractions, list):
                pass
            elif isinstance(self.fractions, float):
                self.fractions = [self.fractions]
            else:
                raise Exception('Given fraction set of incorrect type.')
            if len(self.fractions) == self._nclasses - 1:
                sumf = sum(f for f in self.fractions)
                self.fractions.append(1. - sumf)
            assert (len(self.fractions) == self._nclasses), \
                'Number of fractions <{:d}> doesnt equal number of classes <{:d}>.'.format(len(self.fractions),
                                                                                           self._nclasses)
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
            if isinstance(self.nevents, list):
                pass
            elif isinstance(self.nevents, float):
                self.nevents = [self.nevents]
            else:
                raise Exception('Given nevent set of incorrect type.')
            if not ((len(self.nevents) == self._nclasses) or (len(self.nevents) == self._nclasses - 1)):
                raise Exception('number of provided events <{:d}> does not equal number of classes <{:d}>.'
                                .format(len(self.nevents), self._nclasses))

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
        if self.column in df.columns:
            raise Exception('Column name <{}> already used: <{!s}>. Will not overwrite.'
                            .format(self.column, df.columns))

        # fix final number of events assigned per random class
        # ... each class gets at least one event
        if self.nevents is not None:
            if len(self.nevents) == self._nclasses - 1:
                self.nevents.append(ndf - sum(n for n in self.nevents))
        if self.nevents is None:
            self.nevents = [int(ndf * f) for f in self.fractions]
        for i in range(self._nclasses):
            nsum = sum(n for n in self.nevents[:i + 1])
            ndiff = 0 if nsum - ndf < 0 else nsum - ndf
            self.nevents[i] -= ndiff
        for i, n in enumerate(self.nevents):
            assert n >= 0, 'Random class <{:d}> assigned nevents <{:d}> needs to be greater than zero. {!s}' \
                .format(i, n, self.nevents)
            self.logger.info('Random class <{index:d}> assigned <{n:d}> events.', index=i, n=n)

        # random reshuffling of dataframe indices
        settings = process_manager.service(ConfigObject)
        RNG = RandomState(self._seed)
        permute = RNG.permutation(df.index)

        # apply the random reshuffling, and assign records to the n datasets
        for i in range(self._nclasses):
            ib = sum(n for n in self.nevents[:i])
            ie = sum(n for n in self.nevents[:i + 1])
            df[self.store_key[i]] = df.ix[permute[ib:ie]]
            self.logger.info('Stored output collection <{key}> with <{n:d}> records in datastore.',
                             key=self.store_key[i], n=len(ds[self.store_key[i]].index))

        # increase seed in case of next iteration
        self._seed += 1
        
        return StatusCode.Success
