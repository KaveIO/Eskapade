"""Project: Eskapade - A python-based package for data analysis.

Macro: esk501_fix_pandas_dataframe

Created: 2017/04/26

Description:
    Macro illustrates how to call FixPandasDataFrame link that gives columns
    consistent names and datatypes.
    Default settings perform the following clean-up steps on an
    input dataframe:

    - Fix all column names. Eg. remove punctuation and strange characters,
    and convert spaces to underscores.
    - Check for various possible nans in the dataset, then make all nans
    consistent by turning them into numpy.nan (= float)
    - Per column, assess dynamically the most consistent datatype (ignoring
    all nans in that column). Eg. bool, int, float, datetime64, string.
    - Per column, make the data types of all rows consistent, by using the
    identified (or imposed) data type (by default ignoring all nans)

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import tempfile

from eskapade import ConfigObject, Chain
from eskapade import core_ops, analysis, data_quality
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk501_fix_pandas_dataframe')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk501_fix_pandas_dataframe'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# dummy dataframe filled with inconsistent data types per column

tmp = b"""A,B,C,D,E,F,G,H
True,foo,1.0,1,1,1,a,a
False,bar,2.0,2,2,2.5,b,b
nan,3,bal,3,bla,bar,c,1
,nan,NaN,NaN,nan,nan,d,2
,,,,,,,3
1,2,,,,,,,6
"""

f = tempfile.NamedTemporaryFile(delete=False)
f.write(tmp)
f.close()
# file is not immediately deleted because we used delete=False
# used below with f.name

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch = Chain('DataPrep')

# --- 0. pandas read_csv has multiple settings to help reading in of buggy csv's.
#     o The option error_bad_lines=False skips lines with too few or too many values
#     o The option encoding='latin1' interprets most non-standard characters
read_data = analysis.ReadToDf(key='vrh',
                              reader='csv',
                              path=f.name,
                              error_bad_lines=False,
                              encoding='latin1')
ch.add(read_data)

# --- 1. standard setting:
#     o convert all nans to np.nan (= float)
#     o convert all rows in a column to most occuring datatype in that column
fixer = data_quality.FixPandasDataFrame(name='fixer1')
fixer.read_key = 'vrh'
fixer.store_key = 'vrh_fix1'
ch.add(fixer)

# --- 2. force certain columns to specified datatype
fixer = data_quality.FixPandasDataFrame(name='fixer2')
fixer.read_key = 'vrh'
fixer.store_key = 'vrh_fix2'
fixer.var_dtype = {'B': int, 'C': str}
ch.add(fixer)

# --- 3. convert all nans to data type consistent with rest of column
fixer = data_quality.FixPandasDataFrame(name='fixer3')
fixer.read_key = 'vrh'
fixer.store_key = 'vrh_fix3'
fixer.convert_inconsistent_nans = True
# set a specific nan (GREPME) for a given column (G)
fixer.var_nan = {'G': 'GREPME'}
ch.add(fixer)

# --- 4. compare results
pds = core_ops.PrintDs(name='pds2')
pds.keys = ['vrh', 'vrh_fix1', 'vrh_fix2', 'vrh_fix3']
ch.add(pds)

# --- 5. write out fixed dataframe - turned off in this example
# The dataframe will be saved with the numpy writer which will
# restore the dtypes when reloading the dataframe
writedata = analysis.WriteFromDf(name='writer',
                                 key='vrh_fix1',
                                 path='tmp.npz',
                                 )
# ch.add(writedata)

#########################################################################################

logger.debug('Done parsing configuration file esk501_fix_pandas_dataframe')
