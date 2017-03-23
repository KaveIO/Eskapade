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
import logging
from eskapade import core, ProcessManager, ConfigObject, DataStore
from eskapade.core.project_utils import create_arg_parser

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

    # create parser for command-line arguments
    parser = create_arg_parser()
    user_args = parser.parse_args()

    # create config object for settings
    if not user_args.unpickle_config:
        # create new config
        settings = ConfigObject()
    else:
        # read previously persisted settings if pickled file is specified
        conf_path = user_args.config_files.pop(0)
        settings = ConfigObject.import_from_file(conf_path)
    del user_args.unpickle_config

    # set configuration macros
    settings.add_macros(user_args.config_files)

    # set user options
    settings.set_user_opts(user_args)

    # run Eskapade
    core.execution.run_eskapade(settings)

    # start interpreter if requested (--interactive on command line)
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
