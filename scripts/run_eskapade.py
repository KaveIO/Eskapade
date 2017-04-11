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
import IPython
import pandas as pd

from eskapade import core, ProcessManager, ConfigObject, DataStore
from eskapade.core.run_utils import create_arg_parser


def main():
    """Run Eskapade

    Top-level control function for an Eskapade run started from the
    command line.  Arguments specified by the user are parsed and
    converted to settings in the configuration object.  Optionally, an
    interactive IPython session is started when the run is finished.
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
        pd.set_option('display.width', 120)
        pd.set_option('display.max_columns', 50)

        # start interactive session
        log = logging.getLogger(__name__)
        log.info("Continuing interactive session ... press Ctrl+d to exit.\n")
        IPython.embed()

if __name__ == "__main__":
    main()
