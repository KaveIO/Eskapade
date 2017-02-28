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
