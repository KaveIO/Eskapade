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


class AmbiguousFileType(Exception):
    """Exception raised if file type cannot be inferred"""
    def __init__(self, path):
        """Raise the exception

        :param str path: the path to file
        """
        message = 'File {} has ambiguous type, please add an extension or specify the file_type'.format(path)
        Exception.__init__(self, message)


class UnhandledFileType(Exception):
    """Exception raised if file type is not handled"""
    def __init__(self, path, f_ext, file_type):
        """Raise the exception

        :param str path: the path to file
        :param str f_ext: file extension as determined by splitting
            the path string
        :param str file_type: user set file type.
            Options are {'npy', 'npz'}
        """
        message = 'File type: {0} and file extension: {1} for file {2} are not handled'.format(file_type, f_ext, path)
        m2 = '.\n valid types are npy and npz'
        Exception.__init__(self, message + m2)
