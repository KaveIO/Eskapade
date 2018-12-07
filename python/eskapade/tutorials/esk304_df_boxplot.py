"""Project: Eskapade - A python-based package for data analysis.

Macro : esk304_df_boxplot

Created: 2017/02/23

Description:
    Macro shows how to boxplot the content of a dataframe in a nice summary
    pdf file.

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import analysis, visualization
from eskapade import process_manager
from escore.core import persistence
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk304_df_boxplot.')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk304_df_boxplot'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

msg = r"""

The plots and latex files produced by link df_summary can be found in dir:
{path}
"""
logger.info(msg, path=persistence.io_path('results_data', 'report'))

COLUMNS = ['var_a', 'var_b', 'var_c']
SIZE = 10000
VAR_LABELS = dict(var_a='Variable A', var_b='Variable B', var_c='Variable C')
VAR_UNITS = dict(var_b='m/s')
GEN_CONF = dict(var_a=dict(choice=['alpha', 'beta', 'gamma'], dtype=str), var_b=dict(mean=3., std=1.),
                var_c=dict(choice=['delta', 'epsilon', 'zeta', 'eta'], dtype=str))

#########################################################################################
# --- now set up the chains and links based on configuration flags

# create chains
data = Chain('Data')

# add data-generator link to "Data" chain
generator = analysis.BasicGenerator(name='Generate_data',
                                    key='data',
                                    columns=COLUMNS,
                                    size=SIZE,
                                    gen_config=GEN_CONF)
data.add(generator)

# add data-frame summary link to "Boxplot" chain
# can provide labels and units for the variables in the dataset, and set the statistics to print in output file
plot = Chain('BoxPlot')
box_plot = visualization.DfBoxplot(name='Create_stats_overview',
                                   read_key=generator.key,
                                   statistics=['count', 'mean', 'min', 'max', 'std'],
                                   var_labels=VAR_LABELS,
                                   var_units=VAR_UNITS,
                                   column='var_b',
                                   cause_columns=['var_a', 'var_c'],
                                   results_path=persistence.io_path('results_data', 'report'))
plot.add(box_plot)

#########################################################################################

logger.debug('Done parsing configuration file esk304_df_boxplot.')
