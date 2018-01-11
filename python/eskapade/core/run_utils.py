"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/04/11

Description:
    Utilities for Eskapade run

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import argparse

from eskapade.core.definitions import USER_OPTS
from eskapade.core.definitions import USER_OPTS_KWARGS
from eskapade.core.definitions import USER_OPTS_SHORT


def create_arg_parser():
    """Create parser for user arguments.

    An argparse parser is created and returned, ready to parse
    arguments specified by the user on the command line.

    :returns: argparse.ArgumentParser
    """
    # create parser and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('config_files', nargs='+', metavar='CONFIG_FILE', help='configuration file to execute')
    for sec_keys in USER_OPTS.values():
        for opt_key in sec_keys:
            args = ['--{}'.format(opt_key).replace('_', '-')]
            if opt_key in USER_OPTS_SHORT:
                args.append('-{}'.format(USER_OPTS_SHORT[opt_key]))
            parser.add_argument(*args, **USER_OPTS_KWARGS.get(opt_key, {}))

    return parser
