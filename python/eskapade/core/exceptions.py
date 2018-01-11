"""Project: Eskapade - A python-based package for data analysis.

Created: 2016/11/08

Description:
    Eskapade exceptions.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""


class Error(Exception):
    """Base class for all Eskapade core exceptions."""


class UnknownSetting(Error):
    """The user requested an unknown setting."""
