# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : RandomSampleSplitter                                                  *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      RandomSampleSplitter splits an input dataframe into N sub data frames.                                     *
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

from eskapade import ConfigObject
from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class RandomSampleSplitter(Link):
    """
    RandomSampleSplitter splits an input dataframe into a number of sub data-frames. 

    Records are assigned randomly.
    """

    def __init__(self, **kwargs):
        """
        Store the configuration of link RandomSampleSplitter

        :param str name: name of link
        :param str readKey: key of data to read from data store
        :param list storeKey: keys of datasets to store in data store. Number of sub samples equals length of storeKey list.
        :param list fractions: list of fractions (0<fraction<1) of records assigned to the sub samples. Sum can be less than 1. Needs to be set. 
        :param list nevents: list of number of random records assigned to the sub samples. (optional instead of 'fractions')
        """

        Link.__init__(self, kwargs.pop('name', 'RandomSampleSplitter'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             readKey=None,
                             storeKey=None,
                             fractions=None,
                             nevents=False)

        # check residual kwargs. exit if any present. 
        self.check_extra_kwargs(kwargs)

        return

    def initialize(self):
        """ Check and initialize attributes of RandomSampleSplitter """

        # check that these four attributes have been set correctly.
        assert isinstance(self.readKey, str) and len(self.readKey), 'read key not set.'
        if isinstance(self.storeKey, str) and len(self.storeKey):
            self.storeKey = [self.storeKey]
        elif not (isinstance(self.storeKey, list) and len(self.storeKey)):
            raise Exception('storage keys not filled with list of strings. Exit.')
        self._nclasses = len(self.storeKey)
        assert isinstance(self._nclasses, int) and self._nclasses >= 1, \
            'number of random classes need to be int and greater than or equal to 1 <%d>'
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
                'number of fractions <%d> doesnt equal number of classes <%d>.' % (len(self.fractions), self._nclasses)
            for i, f in enumerate(self.fractions):
                assert f >= 0, '<%d> assigned fraction <%f> needs to be greater than zero.' % (i, f)
            # normalize fractions
            sumf = sum(f for f in self.fractions)
            for i, f in enumerate(self.fractions):
                if sumf > 1:
                    self.fractions[i] = f / sumf
                self.log().info('Random class <%d> assigned fraction is <%f>.' % (i, self.fractions[i]))

        # alternatively, check that provided number of events per random class are all okay.
        if self.nevents is not None:
            if isinstance(self.nevents, list):
                pass
            elif isinstance(self.nevents, float):
                self.nevents = [self.nevents]
            else:
                raise Exception('Given nevent set of incorrect type.')
            if not ((len(self.nevents) == self._nclasses) or (len(self.nevents) == self._nclasses - 1)):
                raise Exception('number of provided events <%d> does not equal number of classes <%d>.' % \
                                (len(self.nevents), self._nclasses))
            pass

        # there needs to be a random seed set in the configobject
        settings = process_manager.service(ConfigObject)
        assert 'seed' in settings, 'random seed not set in ConfigObject.'

        return StatusCode.Success

    def execute(self):
        """ Execute RandomSampleSplitter """

        ds = process_manager.service(DataStore)

        # basic checks on contensts of the data frame
        assert self.readKey in list(ds.keys()), 'Key %s not in DataStore.' % self.readKey
        df = ds[self.readKey]
        if not isinstance(df, pd.DataFrame):
            raise Exception('Retrieved object not of type pandas DataFrame.')
        ndf = len(df.index)
        assert ndf > 0, 'dataframe %s is empty.' % self.readKey
        if self.column in df.columns:
            raise Exception('Column name <%s> already used: <%s>. Will not overwrite.' % (self.column, str(df.columns)))

        # fix final number of events assigned per random class
        # ... each class gets at least one event
        if self.nevents is not None:
            if len(self.nevents) == self._nclasses - 1:
                self.nevents.append(ndf - sum(n for n in self.nevents))
        if self.nevents is None:
            self.nevents = [int(ndf * f) for f in self.fractions]
            pass
        for i in range(self._nclasses):
            nsum = sum(n for n in self.nevents[:i + 1])
            ndiff = 0 if nsum - ndf < 0 else nsum - ndf
            self.nevents[i] -= ndiff
            pass
        for i, n in enumerate(self.nevents):
            assert n >= 0, 'Random class <{:d}> assigned nevents <{:d}> needs to be greater than zero. {}'\
                .format(i, n, str(self.nevents))
            self.log().info('Random class <{:d}> assigned n events <{:d}>.'.format(i, n))

        # random reshuffling of dataframe indices
        settings = process_manager.service(ConfigObject)
        RNG = RandomState(settings['seed'])
        permute = RNG.permutation(df.index)

        # apply the random reshuffling, and assign records to the n datasets
        for i in range(self._nclasses):
            ib = sum(n for n in self.nevents[:i])
            ie = sum(n for n in self.nevents[:i + 1])
            df[self.storeKey[i]] = df.ix[permute[ib:ie]]
            self.log().info('Stored output collection <%s> with <%d> records in datastore.' % \
                            (self.storeKey[i], len(ds[self.storeKey[i]].index)))

        return StatusCode.Success
