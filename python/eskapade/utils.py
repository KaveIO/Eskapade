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

import sys
import os
import subprocess
import logging
import matplotlib


ENV_VARS = dict(es_root='ESKAPADE', wd_root='WORKDIRROOT', spark_args='PYSPARK_SUBMIT_ARGS',
                docker='DE_DOCKER', display='DISPLAY')
PROJECT_DIRS = dict(es_root=('es_root', ''), es_python=('es_root', 'python'), es_scripts=('es_root', 'scripts'),
                    es_lib=('es_root', 'lib'), wd_root=('wd_root', ''), es_cxx_make=('es_root', ''))
PROJECT_FILES = dict(py_mods=('es_root', 'es_python_modules.zip'),
                     run_eskapade=('es_scripts', 'run_eskapade.py'),
                     coll_py_mods=('es_scripts', 'collect_python_modules.sh'))
CXX_LIBRARIES = ('', 'roofit')

log = logging.getLogger(__name__)


def set_matplotlib_backend(backend=None, batch=None, silent=True):
    """Set Matplotlib backend

    :param str backend: backend to set
    :param bool batch: require backend to be non-interactive
    :param bool silent: do not raise exception if backend cannot be set
    :raises: RuntimeError
    """

    # determine if batch mode is required
    display = get_env_var('display')
    run_batch = bool(batch) or display is None or not display.startswith(':') or not display[1].isdigit()
    if run_batch:
        matplotlib.interactive(False)

    # check if batch mode can be set if it is requested
    if not batch and batch is not None and run_batch:
        if not silent:
            raise RuntimeError('interactive Matplotlib mode requested, but no display found')
        log.warning('Matplotlib cannot be used interactively; no display found')

    # get Matplotlib backends
    curr_backend = matplotlib.get_backend().lower()
    ni_backends = [nib.lower() for nib in matplotlib.rcsetup.non_interactive_bk]

    # determine backend to be set
    if not backend:
        # try to use current backend
        backend = curr_backend if not run_batch or curr_backend in ni_backends else ni_backends[0]
    backend = str(backend).lower()

    # check if backend is compatible with mode
    if run_batch and backend not in ni_backends:
        if not silent:
            raise RuntimeError('non-interactive Matplotlib backend required, but "{}" requested'.format(str(backend)))
        log.warning('Set Matplotlib backend to "%s"; non-interactive backend required, but "%s" requested',
                    ni_backends[0], backend)
        backend = ni_backends[0]

    # check if backend has to change
    if backend == curr_backend:
        return

    # check if backend can still be set
    if 'matplotlib.pyplot' in sys.modules:
        if not silent:
            raise RuntimeError('cannot set Matplotlib backend: pyplot module already loaded')
        return

    # set backend
    matplotlib.use(backend)


def get_env_var(key):
    """Retrieve Eskapade-specific environment variables

    :param str key: Eskapade-specific key to variable
    :returns: environment variable value
    :rtype: str
    """

    var_name = ENV_VARS[key]
    return os.environ.get(var_name)


def get_dir_path(key):
    """Function to retrieve Eskapade specific directory path

    :param str key: Eskapade specific project directory key
    :return: directory path
    :rtype: str
    """

    dir_comps = PROJECT_DIRS[key]
    return get_env_var(dir_comps[0]) + (('/%s' % dir_comps[1]) if dir_comps[1] else '')


def get_file_path(key):
    """Function to retrieve Eskapade specific directory file path

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


def build_cxx_library(lib_key='', accept_existing=False):
    """Build Eskapade C++ library

    :param str lib_key: key of the library to build (build all if empty)
    :param bool accept_existing: accept existing library if build fails
    """

    # check library key
    lib_key = str(lib_key) if lib_key else ''
    if lib_key not in CXX_LIBRARIES:
        raise AssertionError('library key must be one of {}'.format(str(CXX_LIBRARIES)))

    # determine build target
    target = 'install'
    if lib_key:
        target = '{0:s}-{1:s}'.format(str(lib_key), target)

    # build library
    make_dir = get_dir_path('es_cxx_make')
    make_res = subprocess.run(['make', '-C', make_dir, target], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              universal_newlines=True)

    # check result
    if make_res.returncode != 0:
        lib_path = '{0:s}/libes{1:s}.so'.format(get_dir_path('es_lib'), lib_key)
        if accept_existing and lib_key and os.path.isfile(lib_path):
            log.warning('Failed to build library with target "%s"; using existing version', target)
        else:
            raise RuntimeError('failed to build library with target "{0:s}":\n{1:s}'.format(target, make_res.stderr))
    else:
        log.debug('Built library with target "%s":\n%s', target, make_res.stdout)
