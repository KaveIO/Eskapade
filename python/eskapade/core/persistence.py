# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : IoConfig                                                              *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Utility class and functions to get correct io path,                       *
# *      used for persistence of results                                           *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import os
import re
import glob
import logging
from pkg_resources import resource_string, resource_filename

from collections import defaultdict

# IO locations
IO_LOCS = dict(results='results_dir',
               data='data_dir',
               macros='macros_dir',
               input_data='data_dir',
               records='data_dir',
               ana_results='results_dir',
               ana_plots='results_dir',
               proc_service_data='results_dir',
               results_data='results_dir',
               results_ml_data='results_dir',
               results_config='results_dir',
               tmva='results_dir',
               plots='results_dir',
               templates='templates_dir')

IO_SUB_DIRS = defaultdict(lambda: '',
                          ana_results='{ana_name:s}',
                          ana_plots='{ana_name:s}/plots',
                          proc_service_data='{ana_name:s}/proc_service_data/v{ana_version:s}',
                          results_data='{ana_name:s}/data/v{ana_version:s}',
                          results_ml_data='{ana_name:s}/data/v{ana_version:s}',
                          results_config='{ana_name:s}/config/v{ana_version:s}',
                          tmva='{ana_name:s}/tmva_output/v{ana_version:s}',
                          plots='{ana_name:s}/plots/v{ana_version:s}')

# get logging instance
log = logging.getLogger(__name__)

# function to replace whitespace in names
repl_whites = lambda name: '_'.join(name.split())


def create_dir(dir_path):
    """Function to create directory

    :param str dir_path: directory path
    """

    if os.path.exists(dir_path):
        if os.path.isdir(dir_path):
            return
        log.critical('directory path "%s" exists, but is not a directory', dir_path)
        raise AssertionError('unable to create IO directory')
    log.debug('creating directory "%s"', dir_path)
    os.makedirs(dir_path)


def io_dir(io_type, io_conf):
    """Functions to construct I/O paths

    :param str io_type: type of result to store, e.g. data, macro, results.
    :param io_conf: IO configuration object
    :return: directory path
    :rtype: str
    """

    # check inputs
    if io_type not in IO_LOCS:
        log.critical('unknown IO type: "%s"', str(io_type))
        raise RuntimeError('IO directory found for specified IO type')
    if not IO_LOCS[io_type] in io_conf:
        log.critical('directory for io_type (%s->%s) not in io_conf' % (io_type, IO_LOCS[io_type]))
        raise RuntimeError('io_dir: directory for specified IO type not found in specified IO configuration')

    # construct directory path
    base_dir = io_conf[IO_LOCS[io_type]]
    sub_dir = IO_SUB_DIRS[io_type].format(ana_name=repl_whites(io_conf['analysis_name']),
                                          ana_version=repl_whites(str(io_conf['analysis_version'])))
    dir_path = base_dir + ('/' if sub_dir else '') + sub_dir

    # create and return directory path
    create_dir(dir_path)
    return dir_path


def io_path(io_type, io_conf, sub_path):
    """Functions to construct IO paths

    :param str io_type: type of result to store, e.g. data, macro, results.
    :param io_conf: IO configuration object
    :param str sub_path: sub path to be included in io path
    :return: full path to directory
    :rtype: str
    """

    # check inputs
    if not isinstance(sub_path, str):
        log.critical('Specified sub path/file name must be a string, but has type "{type!s}"'
                     .format(type=type(sub_path).__name__))
        raise TypeError('The sub path/file name in the io_path function must be a string')
    sub_path = repl_whites(sub_path).strip('/')

    # construct path
    full_path = io_dir(io_type, io_conf) + '/' + sub_path
    if os.path.dirname(sub_path):
        create_dir(os.path.dirname(full_path))

    return full_path


def record_file_number(io_conf, file_name_base, file_name_ext):
    """Function to get next prediction-record file number

    :param io_conf: I/O configuration object
    :param str file_name_base: base file name
    :param str file_name_ext: file name extension
    :return: next prediction-record file number
    :rtype: int
    """

    file_name_base = repl_whites(file_name_base)
    file_name_ext = repl_whites(file_name_ext)
    records_dir = io_dir('records', io_conf)
    max_num = -1
    regex = re.compile('.*/%s_(\d*?).%s' % (file_name_base, file_name_ext))
    for file_path in glob.glob('%s/%s_*.%s' % (records_dir, file_name_base, file_name_ext)):
        digit = regex.search(file_path)
        if digit:
            max_num = max(max_num, int(digit.group(1)))

    return max_num + 1


class IoConfig(dict):
    """Configuration object for I/O operations"""

    _conf_items = dict(analysis_name=str,
                       analysis_version=None,
                       results_dir=str,
                       data_dir=str,
                       macros_dir=str,
                       templates_dir=str)

    def __init__(self, **input_config):
        """Initialize IoConfig instance"""

        # check required items
        for key, val_type in self._conf_items.items():
            if key not in input_config:
                log.critical('item "%s" not found in input IO configuration', key)
                raise KeyError('missing item(s) in input configuration for IoConfig')
            if val_type and not isinstance(input_config[key], val_type):
                log.critical('item "%s" has type "%s" ("%s" required)',
                             key, type(input_config[key]).__name__, str.__name__)
                raise TypeError('incorrect type for item(s) in input configuration for IoConfig')

        # initialize dictionary
        dict.__init__(self, **input_config)
        self['analysis_version'] = str(self['analysis_version'])
