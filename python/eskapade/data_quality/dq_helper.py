"""Project: Eskapade - A Python-based package for data analysis.

Module: data_quality.dq_helper

Created: 2017/04/11

Description:
    Data-quality helper functions

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import ast
import re

import numpy as np
import pandas as pd


def cleanup_string(col):
    """Cleanup input string.

    :param col: string to be cleaned up
    :returns: cleaned up string
    :rtype: str
    """
    if not isinstance(col, str):
        return col
    # strip
    new_col = col.strip()
    # replace empty spaces
    new_col = new_col.replace(' ', '_')
    # keep only alphanumeric and _
    new_col = re.sub('[^A-Za-z0-9_]+', '', new_col)
    return new_col


def check_nan(val):
    """Check input value for not a number.

    :param val: value to be checked for nan
    :returns: true if nan
    :rtype: bool
    """
    if pd.isnull(val):
        return True
    if isinstance(val, str):
        val = val.strip()
        if not val or val.lower() == 'none' or val.lower() == 'nan':
            return True
    # from numpy import datetime64
    # if isinstance(val, datetime64):
    #    return val == datetime64('NaT')
    return False


def convert(val):
    """Convert input to interpreted data type.

    :param val: value to be interpreted
    :returns: interpreted value
    """
    try:
        return ast.literal_eval(val)
    except BaseException:
        pass
    return val


def to_str(val, **kwargs):
    """Convert input to string.

    :param val: value to be converted
    :returns: converted value
    :rtype: str
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except BaseException:
        pass
    if isinstance(val, str):
        return val
    if kwargs.get('convert_inconsistent_dtypes', True):
        if hasattr(val, '__str__'):
            return str(val)
    return kwargs['nan']


def to_int(val, **kwargs):
    """Convert input to int.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.int64
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except BaseException:
        pass
    if isinstance(val, (np.int64, int)):
        return np.int64(val)
    if kwargs.get('convert_inconsistent_dtypes', True):
        try:
            return np.int64(val)
        except BaseException:
            pass
    return kwargs['nan']


def to_float(val, **kwargs):
    """Convert input to float.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.float64
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except BaseException:
        pass
    if isinstance(val, (np.float64, float)):
        return np.float64(val)
    if kwargs.get('convert_inconsistent_dtypes', True):
        try:
            return np.float64(val)
        except BaseException:
            pass
    return kwargs['nan']


def to_date_time(val, **kwargs):
    """Convert input to numpy.datetime64.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: numpy.datetime64
    """
    return pd.to_datetime(val, errors='coerce')


def bool_to_str(val, **kwargs):
    """Convert input boolean to str.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: str
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except BaseException:
        pass
    if isinstance(val, (np.bool_, bool)):
        return str(val)
    if kwargs.get('convert_inconsistent_dtypes', True):
        if hasattr(val, '__str__'):
            return str(val)
    return kwargs['nan']


def bool_to_int(val, **kwargs):
    """Convert input boolean to int.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.int64
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except BaseException:
        pass
    if isinstance(val, (np.bool_, bool)):
        return np.int64(val)
    if kwargs.get('convert_inconsistent_dtypes', False):
        try:
            return np.int64(val)
        except BaseException:
            pass
    return kwargs['nan']


CONV_FUNCS = {str: to_str,
              int: to_int,
              np.int64: to_int,
              bool: bool_to_str,
              np.bool_: bool_to_str,
              float: to_float,
              np.float64: to_float,
              np.datetime64: to_date_time}
