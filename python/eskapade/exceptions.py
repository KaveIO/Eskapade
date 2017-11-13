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


class MissingRootError(MissingPackageError):
    """Exception raised if ROOT is missing."""

    def __init__(self, message='', required_by=''):
        """Set missing-package arguments.

        :param str message: message to show when raised
        :param str required_by: info on component that requires the package
        """
        mess = message if message else 'unable to import ROOT analysis framework'
        MissingPackageError.__init__(self, message=mess, required_by=required_by)


class MissingRooFitError(MissingPackageError):
    """Exception raised if RooFit is missing."""

    def __init__(self, message='', required_by=''):
        """Set missing-package arguments.

        :param str message: message to show when raised
        :param str required_by: info on component that requires the package
        """
        mess = message if message else 'found ROOT, but RooFit is not installed'
        MissingPackageError.__init__(self, message=mess, required_by=required_by)


class MissingSparkError(MissingPackageError):
    """Exception raised if Spark is missing."""

    def __init__(self, message='', required_by=''):
        """Set missing-package arguments.

        :param str message: message to show when raised
        :param str required_by: info on component that requires the package
        """
        mess = message if message else 'unable to import Spark framework'
        MissingPackageError.__init__(self, message=mess, required_by=required_by)


class MissingPy4jError(MissingPackageError):
    """Exception raised if Py4J is missing."""

    def __init__(self, message='', required_by=''):
        """Set missing-package arguments.

        :param str message: message to show when raised
        :param str required_by: info on component that requires the package
        """
        mess = message if message else 'unable to import Py4J framework'
        MissingPackageError.__init__(self, message=mess, required_by=required_by)


class MissingRooStatsError(MissingPackageError):
    """Exception raised if RooStats is missing."""

    def __init__(self, message='', required_by=''):
        """Set missing-package arguments.

        :param str message: message to show when raised
        :param str required_by: info on component that requires the package
        """
        mess = message if message else 'found ROOT, but RooStats is not installed'
        MissingPackageError.__init__(self, message=mess, required_by=required_by)
