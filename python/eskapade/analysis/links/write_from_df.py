"""Project: Eskapade - A python-based package for data analysis.

Class: WriteFromDf

Created: 2016/11/08

Description:
    Algorithm to write a DataFrame from the DataStore to disk

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import copy
import os

import pandas as pd

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.core import persistence
from eskapade.logger import Logger

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
logger = Logger()


class WriteFromDf(Link):
    """Write a DataFrame from the DataStore to disk."""

    def __init__(self, **kwargs):
        """Store the configuration of the link.

        :param str name: Name given to the link
        :param str key: the DataStore key
        :param str path: path where to save the DataFrame
        :param writer: file extension that can be written by a pandas writer function from pd.DataFrame.
            For example: 'csv'
        :param dict dictionary: keys (as in the arg above) and paths (as in the arg above)
            it will write out all the keys to the associated paths.
        :param bool add_counter_to_name: if true, add an index to the output file name.
            Useful when running in loops. Default is false.
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
        """Initialize the link."""
        # perform basic checks of configured attributes
        # a key and path OR dictionary need to have been set.
        if self.path and self.key:
            self.dictionary = {self.key: self.path}
        elif not self.dictionary:
            raise Exception('Path and key OR dictionary not properly set.')

        # correct the output paths, if need be
        paths = list(self.dictionary.values())
        assert '' not in paths, 'One or more of the paths in dict is empty.'
        assert all([isinstance(p, str) for p in paths]), 'One or more of the paths in dict is not string.'
        # update paths if needed
        for k, p in self.dictionary.items():
            if not p.__contains__('/'):
                self.dictionary[k] = persistence.io_path('results_data', p)
                self.logger.debug('Output filename for key <{key}> has been reset to {new_key}.',
                                  key=k, new_key=self.dictionary[k])
        self.logger.info('kwargs passed on to pandas writer are: {kwargs}.', kwargs=self.kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Pick up the dataframe and write to disk.
        """
        ds = process_manager.service(DataStore)

        # check that all dataframes are present
        assert all(k in ds for k in self.dictionary), 'key(s) not in DataStore.'

        # check that all ds items are dataframes
        assert all(isinstance(ds[k], pd.DataFrame) for k in self.dictionary), \
            'key(s) is not a pandas DataFrame.'

        # collect writer and store the dataframes
        for k, path in self.dictionary.items():
            df = ds[k]
            if self.add_counter_to_name:
                ps = os.path.splitext(path)
                path = ps[0] + '_' + str(self._counter) + ps[1]
            writer = pandas_writer(path, self.writer)
            folder = os.path.dirname(path)
            self.logger.debug('Checking for directory <{dir}>.', dir=folder)
            if not os.path.exists(folder):
                self.logger.fatal('Path given is invalid.')
            self.logger.debug('Writing file "{path}".', path=path)
            writer(df, path, **self.kwargs)

        self._counter += 1
        return StatusCode.Success


def pandas_writer(path, writer):
    """Pick the correct pandas writer.

    Based on provided writer setting, or based on file extension.
    """
    if isinstance(writer, str):
        try:
            writer = pd_writers.get(writer)
        except Exception:
            ValueError('writer parameter should be an extension pandas can write to disk.')
    if not writer:
        writer = pd_writers.get(os.path.splitext(path)[1].strip('.'), None)
    if not writer:
        logger.fatal('No suitable writer found for file "{path}".', path=path)
        raise RuntimeError('Unable to find suitable Pandas writer.')
    logger.debug('Using Pandas writer "{writer!s}"', writer=writer)
    return writer
