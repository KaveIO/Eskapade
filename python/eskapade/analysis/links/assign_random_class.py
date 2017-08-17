# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : AssignRandomClass                                                     *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to randomly assign records to a number of class                 *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from numpy.random import RandomState
from pandas import DataFrame

from eskapade import process_manager
from eskapade import ConfigObject
from eskapade import StatusCode
from eskapade import DataStore
from eskapade import Link


class AssignRandomClass(Link):
    """
    AssignRandomClass randomly chooses a number of records that are to be added to the top X records that are
    returned to a client. In this way a top X also has a certain set of randomly chosen records. 
    E.g. these records make any overtraining less likely to happen.
    """

    def __init__(self, **kwargs):
        """
        Store the configuration of link AssignRandomClass

        :param str name: name of link
        :param str readKey: key of data to read from data store
        :param str column: name of new column that specifies the randomly assigned class. Default is randomclass.
        :param int nclasses: number of random classes. Needs to be set. 
        :param list fractions: list of fractions of random records assigned to n classes. Needs to be set. Can be one less than n classes.
        :param list nevents: list of number of random records assigned to n classes. Can be one less than n classes. (optional instead of 'fractions')
        """
        
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'AssignRandomClass'))
        
        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             readKey=None,
                             column='randomclass',
                             fractions=None,
                             nevents=False,
                             nclasses=None)
        
        # check residual kwargs. exit if any present. 
        self.check_extra_kwargs(kwargs)
        
        return

    def initialize(self):
        """ Check and initialize attributes of AssignRandomClass """

        # check that these four attributes have been set correctly.
        assert isinstance(self.nclasses, int) and self.nclasses > 1, 'number of random classes need to be int and ' \
            'greater than 1 <%d>'
        assert isinstance(self.readKey, str) and len(self.readKey), 'read key not set.'
        assert isinstance(self.column, str) and len(self.column), 'column not set.'        
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

            if len(self.fractions) == (self.nclasses - 1):
                sumf = sum(f for f in self.fractions)
                self.fractions.append(1. - sumf)
            assert (len(self.fractions) == self.nclasses), 'number of fractions <%d> doesnt equal number ' \
                                                           'of classes <%d>.' % (len(self.fractions), self.nclasses)
            for i,f in enumerate(self.fractions):
                assert f >= 0, '<%d> assigned fraction <%f> needs to be greater than zero.' % (i,f)
            # normalize fractions
            sumf = sum(f for f in self.fractions)
            for i, f in enumerate(self.fractions):
                self.fractions[i] = f / sumf
                pass
            pass

        # alternatively, check that provided number of events per random class are all okay.
        if self.nevents is not None:
            if isinstance(self.nevents,list):
                pass
            elif isinstance(self.nevents,float):
                self.nevents = [self.nevents]
            else:
                raise Exception('Given nevent set of incorrect type.')
            if not ((len(self.nevents) == self.nclasses) or (len(self.nevents) == self.nclasses - 1)):
                raise Exception('number of provided events <%d> does not equal number of classes <%d>.' % \
                                (len(self.nevents), self.nclasses))
            pass
        
        # there needs to be a random seed set in the configobject
        settings = process_manager.service(ConfigObject)
        assert 'seed' in settings, 'random seed not set in ConfigObject.'

        return StatusCode.Success

    def execute(self):
        """ Execute AssignRandomClass """

        ds = process_manager.service(DataStore)

        # basic checks on contensts of the data frame
        assert self.readKey in list(ds.keys()), 'Key %s not in DataStore.' % self.readKey
        df = ds[self.readKey]
        if not isinstance(df, DataFrame):
            raise Exception('Retrieved object not of type pandas DataFrame.')
        ndf = len(df.index)
        assert ndf > 0, 'dataframe %s is empty.' % self.readKey
        if self.column in df.columns:
            raise Exception('Column name <%s> already used: <%s>. Will not overwrite.' % (self.column, str(df.columns)))

        # fix final number of events assigned per random class
        # ... each class gets at least one event
        if self.nevents is not None:
            if len(self.nevents) == self.nclasses - 1:
                self.nevents.append(ndf - sum(n for n in self.nevents)) 
        if self.nevents is None:
            self.nevents = [int(ndf*f) for f in self.fractions]
            pass
        for i in range(self.nclasses):
            nsum = sum(n for n in self.nevents[:i + 1])
            ndiff = 0 if (nsum - ndf < 0) else (nsum - ndf)
            self.nevents[i] -= ndiff 
            if self.nevents[i] < 0:
                self.nevents[i] = 0
            pass
        for i, n in enumerate(self.nevents):
            assert n >= 0, 'Random class <%d> assigned nevents <%d> needs to be greater than zero. %s' % \
                                                                                        (i, n, str(self.nevents))
            self.log().info('Random class <%d> assigned n events <%d>.' % (i, n))
            
        # random reshuffling of dataframe indices
        settings = process_manager.service(ConfigObject)
        RNG = RandomState(settings['seed'])
        permute = RNG.permutation(df.index)

        # apply the random reshuffling, and assign records to the n classes
        df[self.column] = 0
        for i in range(self.nclasses):
            ib = sum(n for n in self.nevents[:i])
            ie = sum(n for n in self.nevents[:i + 1])
            df.ix[permute[ib:ie],self.column] = i
            pass
        
        return StatusCode.Success

