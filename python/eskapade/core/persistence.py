"""Project: Eskapade - A python-based package for data analysis.

Created: 2016/11/08

Description:
    Utility class and functions to get correct io path,
    used for persistence of results

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import glob
import os
import re
from collections import defaultdict

from eskapade.core.process_manager import process_manager
from eskapade.core.process_services import ConfigObject
from eskapade.logger import Logger

# IO locations
IO_LOCS = dict(config='config_dir',
               results='results_dir',
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
logger = Logger()


def repl_whites(name):
    """Replace whitespace in names."""
    return '_'.join(name.split())


def create_dir(dir_path):
    """Create directory.

    :param str dir_path: directory path
    """
    if os.path.exists(dir_path):
        if os.path.isdir(dir_path):
            return
        logger.fatal('Directory path "{path}" exists, but is not a directory.', path=dir_path)
        raise AssertionError('Unable to create IO directory.')
    logger.debug('Creating directory "{path}".', path=dir_path)
    os.makedirs(dir_path)


def io_dir(io_type, io_conf=None):
    """Construct directory path.

    :param str io_type: type of result to store, e.g. data, macro, results.
    :param io_conf: IO configuration object
    :return: directory path
    :rtype: str
    """
    if not io_conf:
        io_conf = process_manager.service(ConfigObject).io_conf()
    # check inputs
    if io_type not in IO_LOCS:
        logger.fatal('Unknown IO type: "{type!s}".', type=io_type)
        raise RuntimeError('IO directory found for specified IO type.')
    if IO_LOCS[io_type] not in io_conf:
        logger.fatal('Directory for io_type ({type}->{path}) not in io_conf.', type=io_type, path=IO_LOCS[io_type])
        raise RuntimeError('io_dir: directory for specified IO type not found in specified IO configuration.')

    # construct directory path
    base_dir = io_conf[IO_LOCS[io_type]]
    sub_dir = IO_SUB_DIRS[io_type].format(ana_name=repl_whites(io_conf['analysis_name']),
                                          ana_version=repl_whites(str(io_conf['analysis_version'])))
    dir_path = base_dir + ('/' if base_dir[-1] != '/' else '') + sub_dir

    # create and return directory path
    create_dir(dir_path)
    return dir_path


def io_path(io_type, sub_path, io_conf=None):
    """Construct directory path with sub path.

    :param str io_type: type of result to store, e.g. data, macro, results.
    :param str sub_path: sub path to be included in io path
    :param io_conf: IO configuration object
    :return: full path to directory
    :rtype: str
    """
    if not io_conf:
        io_conf = process_manager.service(ConfigObject).io_conf()
    # check inputs
    if not isinstance(sub_path, str):
        logger.fatal('Specified sub path/file name must be a string, but has type "{type!s}"',
                     type=type(sub_path).__name__)
        raise TypeError('The sub path/file name in the io_path function must be a string')
    sub_path = repl_whites(sub_path).strip('/')

    # construct path
    full_path = io_dir(io_type, io_conf) + '/' + sub_path
    if os.path.dirname(full_path):
        create_dir(os.path.dirname(full_path))

    return full_path


def record_file_number(file_name_base, file_name_ext, io_conf=None):
    """Get next prediction-record file number.

    :param str file_name_base: base file name
    :param str file_name_ext: file name extension
    :param io_conf: I/O configuration object
    :return: next prediction-record file number
    :rtype: int
    """
    if not io_conf:
        io_conf = process_manager.service(ConfigObject).io_conf()
    file_name_base = repl_whites(file_name_base)
    file_name_ext = repl_whites(file_name_ext)
    records_dir = io_dir('records', io_conf)
    max_num = -1
    regex = re.compile('.*/{}_(\d*?).{}'.format(file_name_base, file_name_ext))
    for file_path in glob.glob('{}/{}_*.{}'.format(records_dir, file_name_base, file_name_ext)):
        digit = regex.search(file_path)
        if digit:
            max_num = max(max_num, int(digit.group(1)))

    return max_num + 1
