#!/usr/bin/env python3

# **************************************************************************************
# * Project: Eskapade - A python-based package for data analysis                       *
# * Script  : run_eskapade.py                                                          *
# * Created : 2015-09-16                                                               *
# *                                                                                    *
# * Description:                                                                       *
# *      Top-level control script for all commands/run-conditions                      *
# *                                                                                    *
# * Authors:                                                                           *
# *      Eskapade group                                                                *
# *                                                                                    *
# * Redistribution and use in source and binary forms, with or without                 *
# * modification, are permitted according to the terms listed in the file              *
# * LICENSE.                                                                           *
# **************************************************************************************

import os
import re
import logging
from eskapade import core, ProcessManager, ConfigObject, DataStore
from eskapade.core.project_utils import create_parser, arg_setter

if __name__ == "__main__":
    """
    Top level control script for all command-line/run-conditions of Eskapade,
    as run from the command line. 

    The working principle of Eskapade is to run chains of custom code chunks
    (so-called links). 

    Each chain should have a specific purpose, for example pre-processing incoming data, 
    booking and/or training predictive algorithms, validating these predictive algorithms, 
    evaluating the algorithms. By using this principle, links can be easily reused in future projects.
    """

    # create config object for settings
    settings = ConfigObject()

    # set default value for batch-mode switch, based on display settings
    display = core.project_utils.get_env_var('display')
    settings['batchMode'] = display is None or not re.search(':\d', display)

    # Create the argument parser that reads command line arguments.
    parser = create_parser(settings)
    user_args = parser.parse_args()

    # Process all cmd line arguments/options

    # First mandatory user-defined configuration file
    # (settings set in the configuration file supersede cmd line arguments.) 
    settings['macro'] = os.path.abspath(user_args.configFile[0])

    # Then all optional cmd line settings (these may overwrite the macro)
    settings = arg_setter(user_args, settings)

    # Run Eskapade code here

    # perform import here, else help function of argparse does not work correctly
    core.execution.run_eskapade(settings)

    # if requested (-i), end execution in interpreter.
    if settings.get('interactive'):
        # create process manager, config object, and data store
        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        # set Pandas display options
        import pandas as pd
        pd.set_option('display.width', 120)
        pd.set_option('display.max_columns', 50)

        # enable tab completion (and fix terminal input at the same time)
        import rlcompleter, readline
        readline.parse_and_bind('tab: complete')

        # start interactive session
        log = logging.getLogger(__name__)
        log.info("Continuing interactive session ... press Ctrl+d to exit.\n")
        from IPython import embed
        embed()
