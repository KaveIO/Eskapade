"""Project: Eskapade - A python-based package for data analysis.

Macro: Tutorial_1

Created: 2017/02/18

Description:
    Macro illustrates basic setup of chains and links,
    by showing: how to open and run over a dataset,
    apply transformations to it, and plot the results.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pandas as pd

from eskapade import analysis, process_manager, visualization, ConfigObject, Chain, resources

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'Tutorial_1'

#########################################################################################
# --- analysis values, settings, helper functions, configuration flags.

VAR_LABELS = dict(doy='Day of year', date='Date', vis='Visibility', vis_km='Visibility')
VAR_UNITS = dict(vis='mi', vis_km='km')


def comp_date(day):
    """Get date/time from day of year."""
    import pandas as pd
    return pd.Timestamp('1976-01-01') + pd.Timedelta('{:d}D'.format(day - 1))


def mi_to_km(dist):
    """Convert miles to kilometres."""
    return dist * 1.60934


conv_funcs = [{'func': comp_date, 'colin': 'doy', 'colout': 'date'},
              {'func': mi_to_km, 'colin': 'vis', 'colout': 'vis_km'},
              # {'func': mph_to_kph, 'colin': , 'colout': 'wind_kph'},
              # {'func': F_to_C, 'colin': , 'colout': 'temp_c'},
              ]

# this data set is found under: python/eskapade/data/
file_path = resources.fixture('LAozone.data.gz')

#########################################################################################
# --- now set up the chains and links based on configuration flags

# create first chain
data = Chain('Data')

# add data-frame reader to "Data" chain
reader = analysis.ReadToDf(name='Read_LA_ozone', path=file_path, reader=pd.read_csv, key='data')
data.add(reader)

# add conversion functions to "Data" chain
transform = analysis.ApplyFuncToDf(name='Transform', read_key=reader.key, store_key='transformed_data',
                                   apply_funcs=conv_funcs)
data.add(transform)

# create second chain
summary = Chain('Summary')

# add data-frame summary link to "Summary" chain
summarizer = visualization.DfSummary(name='Create_stats_overview', read_key=transform.store_key,
                                     var_labels=VAR_LABELS, var_units=VAR_UNITS)
summary.add(summarizer)


#########################################################################################
# --- exercises
#
# 1.
# Run the macro and take a look at the output.

# 2.
# Now add your own transformation to the ApplyFuncToDf class.
# We want to transform the temperature to Celsius, so use the code in the comments and fill it out.
# As you can see the output will be written in the DataFrame to the column 'temp_c'.
# We also want to include in the plot the wind speed in km/h.
# Rerun the macro and take a look at the output. The output can be found in decision_engine/results

# 3.
# We are going to make a new link by calling eskapade_generate_link command.
# Place the link in links/tutoriallinks and since the link will be doing a transformation, name it something
# appropriate. Write a link that calls the datastore, picks up the dataframe and adds a new column that contains the
# wind speed in km/h. If this works try to add the temperature in degrees Celsius.

# 4.
# Now we add a new link using templates. In the tutorial you can read how.
# Now we are going to make a completely different transformation in the Link and apply it to the object
# in the DataStore. We want to do add a column to the data that states how humid it is.
# When column 'humidity' is less than 50 it is 'dry', otherwise it is 'humid'.
# You will have to use some pandas functionality or perhaps something else if you prefer. Save the
# new column back into the DataFrame and then put the DataFrame in the DataStore under the key 'data_new'.
# DO NOT USE THE ApplyFuncToDf FOR THIS.

# 5.
# Now rerun the full macro and compare the output with the original output. If you have overwritten over the original
# output, you can add version with settings['version'] = 1 to generate a new output folder.
