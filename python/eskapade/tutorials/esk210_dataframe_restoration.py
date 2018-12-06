"""Project: Eskapade - A python-based package for data analysis.

Macro: esk202_writedata

Created: 2017/02/20

Description:
    Macro to illustrate writing pandas dataframes to file and reading
    them back in whilst retaining the datatypes and index using numpy
    and feather file formats.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
import pandas as pd

from eskapade import Chain
from eskapade import DataStore
from eskapade import resources
from eskapade import ConfigObject
from eskapade import process_manager

from eskapade.logger import Logger

from escore.core import persistence

from eskapade.analysis import ReadToDf
from eskapade.analysis import WriteFromDf

logger = Logger()
logger.loglevel = 'DEBUG'
logger.debug('Now parsing configuration file esk210_dataframe_restoration')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk210_dataframe_restoration'
settings['version'] = 0

ds = process_manager.service(DataStore)
#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.
# Two writers that are able to restore dataframes have been included in eskapade.
# To turn one of them off set the below to False
settings['do_numpy'] = True
settings['do_feather'] = True


#########################################################################################
# --- Set path of data
# messy_dtypes is a small files with some complex data types that are
# not guaranteed to be read back properly when using csv
data_path = resources.fixture('messy_dtypes.csv')

# The actual fundamental data types are:
dtypes = [
    'str',
    'int64',
    'float32',
    'float64',
    'S32',
    'str',
    'bool',
    'str',
    'uint64',
    'str'
]

#   Inferred  |   True
# -----------------------
# 'object'    | 'str',
# 'int64'     | 'int64'
# 'float64'   | 'float32'
# 'float64'   | 'float64'
# 'object'    | 'S32'
# 'int64'     | 'str'
# 'bool'      | 'bool'
# 'bool'      | 'str'
# 'uint64'    | 'uint64'
# 'uint64'    | 'str'

# As one can see the dtypes are not what they were before writing to
# disk. Whilst pandas type inference is pretty impressive its not perfect
# The data above that weird type on purpose to show what kind of differences
# one might encounter when relying on pandas inference

# We will load the data from disk using pandas
df_raw = pd.read_csv(data_path).drop('Unnamed: 0', 1)

# We quickly restore the dtypes and then show how numpy and feather
# can be used to prevent this from happening
for i, col in enumerate(df_raw.columns):
    df_raw.loc[:, col] = df_raw.loc[:, col].astype(dtypes[i], copy=False)

# We give the dataframe a non-numerical index to show the functionality
# in this regard
df_raw.set_index('strings', drop=True, inplace=True)

ds['typed_data'] = df_raw
# ****************************************************************************
#                                     TLDR
# ****************************************************************************
# The numpy and feather formats have numerous benefits over traditional formats

# The main benefits are:
#    * Speed
#    * dtype restoration
#    * automated index restoration
#    * No size limit, i.e. pickle doesn't work with files over 4 GB

# In general I would advise to use:
#    * Numpy npy for smaller files
#    * Numpy npz for larger files
#    * Feather when working with people that use R
# *****************************************************************************

###############################################################################
# --- Numpy reader - writer (R/W)

# The numpy R/W has two file types:
#    * npy : essentially a numpy array in byte format
#    * npz : a collection of numpy arrays in a compressed byte format

# The primary benefits of the numpy W/R are:
#    * High performance; for larger files the reader is +- 30x faster
#    * Data type restoration
#    * Non-numerical index restoration
#    * Assigns native numpy dtypes which are those used pandas

if settings['do_numpy']:
    nr = Chain('numpy_read')

    # Write the dataframe from the datastore to disk

    # The numpy writer has some additional functionality
    #
    # Parameters
    # ----------
    #   writer: str
    #       For example: 'csv' will trigger the DataFrame.to_csv.
    #       To use numpy_writer specify one of the following:
    #           {'numpy', 'np', 'npy', 'npz', }
    #       If writer is not passed the path must contain a known file
    #       extension. Valid numpy extensions {'npy', 'npz'}
    #
    #       The numpy and feather writers will preserve the
    #       metadata such as dtypes for each column and the index
    #       if non numeric.
    #
    #   store_index : bool
    #       whether the index should be stored as metadata.
    #       Default is False unless the index is non-numeric

    nr.add(
        WriteFromDf(
            name='numpy_writer',
            key='typed_data',
            path='tmp_tut_esk210.npy',
        )
    )

    # --- Read the dataframe from disk restoring the dtypes and index

    nw = Chain('numpy_write')
    # The numpy reader has some additional functionality
    #
    # Parameters
    # ----------
    #  reader: str
    #      reader is determined automatically. But can be set by
    #      hand, e.g. csv, xlsx. To use the numpy reader one of the
    #      following should be true:
    #          * reader is {'numpy', 'np', 'npy', 'npz'}
    #          * path contains extensions {'npy', 'npz'}
    #          * param `file_type` is {'npy', 'npz'}
    #  restore_index : bool
    #        whether to store the index in the metadata. Default is
    #        False when the index is numeric, True otherwise.
    #  file_type : str | {'npy', 'npz'}
    #      when using the numpy reader. Optional, see reader for details.

    nw.add(
        ReadToDf(
            name='numpy_reader',
            key='reloaded_typed_data_np',
            path=persistence.io_path('results_data', 'tmp_tut_esk210.npy'),
        )
    )

# The dataframe has now been restored with the dtypes of the original df
# and it's index has been restored as it is non-numeric

#########################################################################################
# --- Feather reader - writer (R/W)

# The primary benefits of the Feather W/R are:
#    * Interoperability with R dataframes; this package was written
#         to make sharing data between R and Python much easier
#    * High performance; relies on the apache Arrow framework
#    * Data type restoration
#    * Non-numerical index restoration

if settings['do_feather']:
    fr = Chain('feather_reader')

    # Write the dataframe from the datastore to disk

    # The Feather writer has some additional functionality
    #
    # Parameters
    # ----------
    #   writer: str
    #       For example: 'csv' will trigger the DataFrame.to_csv.
    #       To use feather specify: {'feather', 'ft'}
    #       If writer is not passed the path must contain a known file
    #       extension which is 'ft' for feather
    #
    #       The numpy and feather writers will preserve the
    #       metadata such as dtypes for each column and the index
    #       if non-numeric.
    #
    #   store_index : bool
    #       whether the index should be stored as metadata.
    #       Default is False unless the index is non-numeric

    fr.add(
        WriteFromDf(
            name='feather_writer',
            key='typed_data',
            path='tmp_tut_esk210.ft',
        )
    )

    # --- Read the dataframe from disk restoring the dtypes and index

    # The Feather reader has some additional functionality
    #
    # Parameters
    # ----------
    #  reader: str
    #      reader is determined automatically. But can be set by
    #      hand, e.g. csv, xlsx.
    #      To use the feather reader one of the following should be true:
    #          * reader is {'feather', 'ft'}
    #          * path contains extensions 'ft'

    #  restore_index : bool
    #        whether to store the index in the metadata. Default is
    #        False when the index is numeric, True otherwise.

    fw = Chain('feather_writer')
    fw.add(
        ReadToDf(
            name='feather_reader',
            key='reloaded_typed_data_ft',
            path=persistence.io_path('results_data', 'tmp_tut_esk210.ft'),
        )
    )

# The dataframe has now been restored with the dtypes of the original df
# and it's index has been restored as it is non-numeric
#########################################################################################

logger.debug('Done parsing configuration file esk202_writedata')
