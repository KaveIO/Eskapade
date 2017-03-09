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
import argparse
import logging
from eskapade import core, ProcessManager, ConfigObject, DataStore

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

    # set some default options
    display = core.project_utils.get_env_var('display')
    runBatch = display is None or not re.search(':\d', display)
    runInterpreter = False
    settings = ConfigObject()
    settings['doCodeProfiling'] = False
    settings['storeResultsEachChain'] = False
    settings['doNotStoreResults'] = False

    # Definition of all options and defaults given as command line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("configFile", nargs="+", help="configuration file to execute")
    parser.add_argument("-L", "--log-level", help="set log level",
                        choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL", "OFF"])
    parser.add_argument("-F", "--log-format", help="format of log messages",
                        default="%(asctime)s %(levelname)s [%(module)s/%(funcName)s]: %(message)s")
    parser.add_argument("-S", "--seed", type=int, help="set the random seed for toy generation",
                        default=settings['seed'])
    parser.add_argument("-B", "--batch-mode", help="run in batch mode, not using X Windows",
                        action="store_true", default=runBatch)
    parser.add_argument("-i", "--interactive", help="remain in interactive mode after running",
                        action="store_true", default=runInterpreter)
    parser.add_argument("-b", "--begin-with-chain", help="begin running from particular chain in chain-list", default="")
    parser.add_argument("-e", "--end-with-chain", help="last chain to run in chain-list", default="")
    parser.add_argument("-s", "--single-chain",   help="select which single chain to run", default="")
    parser.add_argument("-w", "--store-intermediate-result", help="store intermediate result after each chain, not only at end",
                        action="store_true", default=settings['storeResultsEachChain'])
    parser.add_argument("-W", "--store-intermediate-result-one-chain", help="store intermediate result of one chain",
                        default="")
    parser.add_argument("-c", "--cmd", help="python commands to process (semi-colon-seperated)")
    parser.add_argument("-U", "--userArg", help="arbitrary user argument(s)", default="")
    parser.add_argument("-P", "--run-profiling",
                        help="Run a python profiler during main Eskapade execution",
                        action="store_true")
    parser.add_argument("-v", "--data-version", help="use the samples for training containing this version number",
                        type=int, default=0)
    parser.add_argument("-a", "--analysis-name", help="The name of the analysis", default="")
    parser.add_argument("-u", "--unpickle-config", help="Unpickle configuration object from configuration file.",
                        action="store_true", default=False)
    parser.add_argument("-r", "--results-dir", help="Set path of the storage results directory", default="")
    parser.add_argument("-d", "--data-dir", help="Set path of the data directory", default="")
    parser.add_argument("-m", "--macros-dir", help="Set path of the macros directory", default="")
    parser.add_argument("-n", "--do-not-store-results", help="Do not store results in pickle files",
                        action="store_true", default=settings['doNotStoreResults'])

    user_args = parser.parse_args()

    # Process all cmd line arguments/options

    # First mandatory user-defined configuration file
    # (settings set in the configuration file supersede cmd line arguments.) 
    settings['macro'] = os.path.abspath(user_args.configFile[0])

    # Then all optional cmd line settings (these may overwrite the macro)
    if user_args.log_level:     # this fixes the logging level globally
        if user_args.log_level not in core.definitions.LOG_LEVELS:
            raise ValueError("Unknown logging level: %r" % user_args.log_level)
        settings['logLevel'] = core.definitions.LOG_LEVELS[user_args.log_level]
    else:
        settings['logLevel'] = logging.INFO
    settings['logFormat'] = user_args.log_format
    if user_args.seed != 0:  # 0 is default because type is int
        settings['seed'] = user_args.seed
    settings['batchMode'] = bool(user_args.batch_mode)
    if user_args.begin_with_chain and len(user_args.begin_with_chain) > 0:
        settings['beginWithChain'] = user_args.begin_with_chain
    if user_args.end_with_chain and len(user_args.end_with_chain) > 0:
        settings['endWithChain'] = user_args.end_with_chain
    if user_args.single_chain and len(user_args.single_chain) > 0:
        settings['beginWithChain'] = user_args.single_chain
        settings['endWithChain'] = user_args.single_chain
    if user_args.store_intermediate_result:
        settings['storeResultsEachChain'] = user_args.store_intermediate_result
    if user_args.store_intermediate_result_one_chain:
        settings['storeResultsOneChain'] = user_args.store_intermediate_result_one_chain
    if user_args.do_not_store_results:
        settings['doNotStoreResults'] = user_args.do_not_store_results
    if user_args.data_version != 0:
        settings['version'] = user_args.data_version
    if user_args.run_profiling:
        settings['doCodeProfiling'] = True
    if user_args.interactive:
        runInterpreter = True
    if user_args.cmd:
        settings['cmd'] = user_args.cmd
        settings.parse_cmd_options()
    if user_args.userArg:
        settings['userArg'] = user_args.userArg
    if user_args.analysis_name and len(user_args.analysis_name) > 0:
        settings['analysisName'] = user_args.analysis_name
    if len(user_args.results_dir) > 0:
        settings['resultsDir'] = user_args.results_dir
    if len(user_args.data_dir) > 0:
        settings['dataDir'] = user_args.data_dir
    if len(user_args.macros_dir) > 0:
        settings['macrosDir'] = user_args.macros_dir
    if user_args.unpickle_config:
        # load configuration settings from Pickle file
        settings = ConfigObject.import_from_file(user_args.configFile[0])
        
    # Run Eskapade code here

    # perform import here, else help function of argparse does not work correctly
    core.execution.run_eskapade(settings)

    # if requested (-i), end execution in interpreter.
    if runInterpreter:
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
