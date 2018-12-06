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

import os
import copy

import numpy as np
import pandas as pd

from eskapade.logger import Logger
from escore.core import persistence
from eskapade import process_manager, DataStore, Link, StatusCode


def numpy_writer(df, path, store_index):
    """Write df to disk in numpy format; preserving the metadata

    :param DataFrame df: pandas Dataframe to write out
    :param str path: target file location
    :param bool store_index: store index in DataFrame
    """
    # if the index is non-numeric we overwrite the default and store
    store_index = store_index or (df.index.dtype not in (np.int_, int))

    dtypes = df.dtypes.values
    # A column can have type object which can hold more than one
    # fundamental data type, if object we determine the fundamental type
    obj_cols = df.columns.values[dtypes == object]

    fund_dtypes = np.array([
        type(df.iloc[0, i]).__name__ if col in obj_cols else str(dtypes[i])
        for i, col in enumerate(df.columns.values)
    ])

    f_ext = os.path.splitext(path)[1].strip('.')
    if f_ext == 'npz':
        logger.debug('Saving using numpy as npz')
        if store_index:
            np.savez(path,
                     values=df.values,
                     columns=df.columns.values,
                     dtypes=fund_dtypes,
                     index=df.index.values)
        else:
            np.savez(path,
                     values=df.values,
                     columns=df.columns.values,
                     dtypes=fund_dtypes)
    else:
        logger.debug('Saving using numpy as npy')

        if store_index:
            df.index.name = 'restored_index'
            df.reset_index(inplace=True)
            fund_dtypes = np.insert(fund_dtypes, 0, type(df.iloc[0, 0]).__name__)

        col_dtypes = np.hstack((df.columns.values[:, None], fund_dtypes[:, None]))
        np.save(path, np.array((df.values, col_dtypes, np.array([store_index]))))

        if store_index:
            # The above reset_index can propagate back, this remedies that
            df.set_index('restored_index', drop=True, inplace=True)
            df.index.name = 'index'


def feather_writer(df, path, store_index):
    """Write df to disk in feather format; preserving the metadata

    :param DataFrame df: pandas Dataframe to write out
    :param str path: target file location
    :param bool store_index: store index in DataFrame, default is True
    """
    import feather
    if (store_index is False) and (df.index.dtype not in (np.int_, int)):
        logger.info('The non-numerical index will not be stored')

    if store_index:
        df.index.name = 'restored_index'
        df.reset_index(inplace=True)

    dtypes = df.dtypes.values
    # A column can have type `object` which can hold more than one
    # fundamental data type, if object we determine the fundamental type
    obj_cols = df.columns[dtypes == object]
    fund_dtypes = [
        type(df.iloc[0, i]).__name__ if col in obj_cols else str(dtypes[i])
        for i, col in enumerate(df.columns.values)
    ]

    # The underlying Apache framework doesn't handle numpy dtypes
    # we convert to strings and create an array of the right shape
    dtypes_arr = np.zeros(shape=df.shape[0], dtype=np.dtype)
    dtypes_arr[:df.shape[1]] = fund_dtypes
    df['_dtypes'] = dtypes_arr.astype(str)

    logger.debug('Using Feather writer')
    feather.write_dataframe(df, path)

    # The above reset_index and _dtypes column can propogate back, this remedies that
    if store_index:
        df.set_index('restored_index', drop=True, inplace=True)
        df.index.name = 'index'
    df.drop(labels=['_dtypes'], axis=1, inplace=True)


all_writers = {'csv': pd.DataFrame.to_csv,
               'xls': pd.DataFrame.to_excel,
               'xlsx': pd.DataFrame.to_excel,
               'json': pd.DataFrame.to_json,
               'h5': pd.DataFrame.to_hdf,
               'sql': pd.DataFrame.to_sql,
               'htm': pd.DataFrame.to_html,
               'html': pd.DataFrame.to_html,
               'dta': pd.DataFrame.to_stata,
               'pkl': pd.DataFrame.to_pickle,
               'pickle': pd.DataFrame.to_pickle,
               'numpy': numpy_writer,
               'np': numpy_writer,
               'npy': numpy_writer,
               'npz': numpy_writer,
               'feather': feather_writer,
               'ft': feather_writer}

logger = Logger()


class WriteFromDf(Link):
    """Write a DataFrame from the DataStore to disk."""

    def __init__(self, **kwargs):
        """Store the configuration of the link.

        :param str name: Name given to the link
        :param str key: the DataStore key
        :param str path: path where to save the DataFrame
        :param writer: file extension that can be written by a pandas\
        writer function from pd.DataFrame, or the numpy- feather writers.\
        For example: 'csv' will trigger the DataFrame.to_csv.\
        To use numpy_writer specify one of the following:

        {'numpy', \
        'np', \
        'npy', \
        'npz', \
        }

        To use feather specify: {'feather', 'ft'} \
        If writer is not passed the path must contain a known file \
        extension. Valid numpy extensions {'npy', 'npz'} or feather {'ft'}

        :note: the numpy and feather writers will preserve the \
        metadata such as dtypes for each column and the index \
        if non numeric.
        :param dict dictionary: keys (as in the arg above) and paths (as in the arg above) \
        it will write out all the keys to the associated paths.
        :param bool add_counter_to_name: if true, add an index to the output file name. \
        Useful when running in loops. Default is false.
        :param bool store_index: whether the index should be stored as \
        metadata. Default is False unless the index is non-numeric
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
            self.path_map = {self.key: self.path}
        elif not self.path_map:
            raise Exception('Path and key OR dictionary not properly set.')

        # correct the output paths, if need be
        paths = list(self.path_map.values())
        assert '' not in paths, 'One or more of the paths in dict is empty.'
        assert all([isinstance(p, str) for p in paths]), 'One or more of the paths in dict is not string.'
        # update paths if needed
        for k, p in self.path_map.items():
            if not p.__contains__('/'):
                self.path_map[k] = persistence.io_path('results_data', p)
                self.logger.debug('Output filename for key <{key}> has been reset to {new_key}.',
                                  key=k, new_key=self.path_map[k])


        self.logger.info('kwargs passed on to pandas writer are: {kwargs}.', kwargs=self.kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Pick up the dataframe and write to disk.
        """
        ds = process_manager.service(DataStore)

        # check that all dataframes are present
        assert all(k in ds for k in self.path_map), 'key(s) not in DataStore.'

        # check that all ds items are dataframes
        assert all(isinstance(ds[k], pd.DataFrame) for k in self.path_map), \
            'key(s) is not a pandas DataFrame.'

        # Kwarg for numpy and feather writers
        self.store_index = self.kwargs.pop('store_index', True)

        # collect writer and store the dataframes
        for k, path in self.path_map.items():
            df = ds[k]
            if self.add_counter_to_name:
                ps = os.path.splitext(path)
                path = ps[0] + '_' + str(self._counter) + ps[1]
            writer = get_writer(path, self.writer)
            folder = os.path.dirname(path)
            persistence.create_dir(folder)
            self.logger.debug('Checking for directory <{dir}>.', dir=folder)
            if not os.path.exists(folder):
                self.logger.fatal('Path given is invalid.')
            self.logger.info('Writing file "{path}".', path=path)
            if writer == numpy_writer or writer == feather_writer:
                writer(df, path, self.store_index)
            else:
                writer(df, path, **self.kwargs)

        self._counter += 1
        return StatusCode.Success


def get_writer(path, writer, *args, **kwargs):
    """Pick the correct writer.

    Based on provided writer setting, or based on file extension.
    """
    if isinstance(writer, str):
        try:
            writer = all_writers.get(writer)
        except Exception:
            ValueError('writer parameter should be an extension pandas can write to disk.')
    if not writer:
        writer = all_writers.get(os.path.splitext(path)[1].strip('.'), None)
    if not writer:
        logger.fatal('No suitable writer found for file "{path}".', path=path)
        raise RuntimeError('Unable to find suitable writer.')
    logger.debug('Using writer "{writer!s}"', writer=writer)
    return writer
