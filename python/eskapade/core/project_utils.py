# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Utility functions to collect Eskapade python modules                      *
# *      e.g. functions to get correct Eskapade file paths and env variables       *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import os
import argparse
import subprocess

ENV_VARS = dict(es_root='ESKAPADE', wd_root='WORKDIRROOT', spark_args='PYSPARK_SUBMIT_ARGS',
                docker='DE_DOCKER', display='DISPLAY')
PROJECT_DIRS = dict(es_root=('es_root', ''), es_python=('es_root', 'python'), es_scripts=('es_root', 'scripts'),
                    wd_root=('wd_root', ''))
PROJECT_FILES = dict(py_mods=('es_root', 'es_python_modules.zip'),
                     coll_py_mods=('es_scripts', 'collect_python_modules.sh'))


def set_matplotlib_backend():
    """Set Matplotlib backend in batch mode"""

    display = get_env_var('display')
    runBatch = display is None or not display.startswith(':') or not display[1].isdigit()
    if runBatch:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        plt.ioff()


def get_env_var(key):
    """Retrieve Eskapade-specific environment variables

    :param str key: Eskapade-specific key to variable
    :returns: environment variable value
    :rtype: str
    """

    var_name = ENV_VARS[key]
    return os.environ.get(var_name)


def get_dir_path(key):
    """Function to retrieve Eskapade specific directrory path

    :param str key: Eskapade specific project directory key
    :return: directory path
    :rtype: str
    """

    dir_comps = PROJECT_DIRS[key]
    return get_env_var(dir_comps[0]) + (('/%s' % dir_comps[1]) if dir_comps[1] else '')


def get_file_path(key):
    """Function to retrieve Eskapade specific directrory file path

    :param str key: Eskapade specific project file key
    :return: file path
    :rtype: str
    """

    file_comps = PROJECT_FILES[key]
    return get_dir_path(file_comps[0]) + (('/%s' % file_comps[1]) if file_comps[1] else '')


def collect_python_modules():
    """Function to collect Eskapade Python modules"""

    mods_file = get_file_path('py_mods')
    coll_script = get_file_path('coll_py_mods')
    if subprocess.call(['bash', coll_script, mods_file]) != 0:
        raise RuntimeError('Unable to collect python modules')


def create_parser(settings):
    """Create parser for user arguments

    An argparse parser is created and returned, ready to parse
    arguments specified by the user on the command line.

    :param: ConfigObject settings: Eskapade settings
    :return: argparse.ArgumentParser
    """

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
                        action="store_true", default=settings['batchMode'])
    parser.add_argument("-i", "--interactive", help="remain in interactive mode after running",
                        action="store_true", default=settings['interactive'])
    parser.add_argument("-b", "--begin-with-chain", help="begin running from particular chain in chain-list",
                        default="")
    parser.add_argument("-e", "--end-with-chain", help="last chain to run in chain-list", default="")
    parser.add_argument("-s", "--single-chain", help="select which single chain to run", default="")
    parser.add_argument("-w", "--store-intermediate-result",
                        help="store intermediate result after each chain, not only at end",
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

    return parser
