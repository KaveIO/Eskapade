"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/03/31

Description:
    Eskapade exceptions

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""


class MissingPackageError(Exception):
    """Exception raised if third-party package is missing."""

    def __init__(self, message='', required_by=''):
        """Set missing-package arguments.

        :param str message: message to show when raised
        :param str required_by: info on component that requires the package
        """
        mess = message if message else 'missing third-party package'
        if required_by:
            mess += ' (required by "{}")'.format(required_by)
        Exception.__init__(self, mess)
