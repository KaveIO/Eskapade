# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : WriteFromDf                                                    *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to write a DataFrame from the DataStore to disk                 *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import copy
import logging
import os

import pandas as pd

from eskapade import ConfigObject
from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager
from eskapade.core import persistence

pd_writers = {'csv': pd.DataFrame.to_csv,
              'xls': pd.DataFrame.to_excel,
              'xlsx': pd.DataFrame.to_excel,
              'json': pd.DataFrame.to_json,
              'h5': pd.DataFrame.to_hdf,
              'sql': pd.DataFrame.to_sql,
              'htm': pd.DataFrame.to_html,
              'html': pd.DataFrame.to_html,
              'dta': pd.DataFrame.to_stata,
              'pkl': pd.DataFrame.to_pickle,
              'pickle': pd.DataFrame.to_pickle}
log = logging.getLogger(__name__)


class WriteFromDf(Link):
    """
    Write a DataFrame from the DataStore to disk.
    """

    def __init__(self, **kwargs):
        """
        Store the configuration of link WriteFromDf

        :param str name: Name given to the link
        :param str key: the DataStore key
        :param str path: path where to save the DataFrame
        :param writer: file extension that can be written by a pandas writer function from pd.DataFrame. For example: 'csv'
        :param dict dictionary: keys (as in the arg above) and paths (as in the arg above) it will write out all the keys
            to the associated paths.
        :param bool add_counter_to_name: if true, add an index to the output file name. Useful when running in loops. Default is false.
        :param kwargs: all other key word arguments are passed on to the pandas writers.
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'WriteFromDf'))
                
        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs, path='', key='', writer=None, dictionary={}, add_counter_to_name=False)

        # pass on remaining kwargs to pandas writer
        self.kwargs = copy.deepcopy(kwargs)

        # execute counter
        self._counter = 0
        
        return

    def initialize(self):
        """ Initialize WriteFromDf """

        # perform basic checks of configured attributes
        # a key and path need to have been set.
        if self.key == '' and self.path == '' and self.dictionary is None:
            raise Exception('Key, path and dictionary are not set.')
        if len(self.key) == 0 and len(self.dictionary) == 0:
            raise Exception('Key or dict has not been set.')
        if len(self.path) == 0 and len(self.dictionary) == 0:
            raise Exception('Output filename or dict has not been set. Exit.')
        else:
            assert self.path != '' and isinstance(self.path, str), 'path not given.'
        if self.path and self.key:
            self.dictionary = {self.key: self.path}
        elif self.dictionary:
            pass
        else:
            raise Exception('Path and key OR dictionary not properly set.')
        
        # correct the output paths, if need be
        if self.dictionary:
            paths = list(self.dictionary.values())
            assert '' not in paths, 'One or more of the paths in dict is empty.'
            assert False not in [isinstance(p, str) for p in paths]
            # update paths if needed
            for k in self.dictionary.keys():
                p = self.dictionary[k]
                if not p.__contains__('/'):
                    io_conf = process_manager.service(ConfigObject).io_conf()
                    self.dictionary[k] = persistence.io_path('results_data', io_conf, p)
                    self.log().debug('Output filename for key <%s> has been reset to: %s' % (k,self.dictionary[k]))

        self.log().info('kwargs passed on to pandas writer are: %s' % self.kwargs )
        
        return StatusCode.Success

    def execute(self):
        """ Execute WriteFromDf

        Pick up the dataframe and write to disk.
        """

        ds = process_manager.service(DataStore)

        # check that all dataframes are present
        assert all(k in list(ds.keys()) for k in list(self.dictionary.keys())), 'key(s) not in DataStore.'

        # check that all ds items are dataframes
        assert all(isinstance(ds[k],pd.DataFrame) for k in list(self.dictionary.keys())), \
            'key(s) is not a pandas DataFrame.'
        
        # collect writer and store the dataframes
        for k in list(self.dictionary.keys()):
            df = ds[k]
            path = self.dictionary[k]
            if self.add_counter_to_name:
                ps = os.path.splitext(path)
                path = ps[0] + '_' + str(self._counter) + ps[1]
            writer = pandas_writer(path, self.writer)
            folder = os.path.dirname(path)
            self.log().debug('Checking for directory: {}'.format(folder))
            if not os.path.exists(folder):
                self.log().fatal('Path given is invalid.')
            self.log().debug('Writing file: {}'.format(path))
            writer(df, path, **self.kwargs)

        self._counter += 1
        return StatusCode.Success


def pandas_writer(path, writer):
    """ 
    Pick the correct pandas writer.

    Based on provided writer setting, or based on file extension.
    """
    if isinstance(writer, str):
        try:
            writer = pd_writers.get(writer)
        except:
            ValueError('writer parameter should be an extension pandas can write to disk.')
    if not writer:
        writer = pd_writers.get(os.path.splitext(path)[1].strip('.'), None)
    if not writer:
        log.critical('No suitable writer found for file "%s"', path)
        raise RuntimeError('Unable to find suitable Pandas writer')
    log.debug('Using Pandas writer "%s"', str(writer))
    return writer
