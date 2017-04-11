import pandas as pd
import numpy as np

global ast
import ast


def check_nan(x):
    """Check input value for not a number

    :param x: value to be checked for nan
    :returns: true if nan
    :rtype: bool
    """
    if pd.isnull(x):
        return True
    if isinstance(x, str):
        x = x.strip()
        if not x or x.lower() == 'none' or x.lower() == 'nan':
            return True
    #from numpy import datetime64
    # if isinstance(x, datetime64):
    #    return x == datetime64('NaT')
    return False


def convert(x):
    """Convert input to interpreted data type

    :param x: value to be interpreted
    :returns: interpreted value
    """
    try:
        return ast.literal_eval(x)
    except:
        pass
    return x


def to_str(val, **kwargs):
    """Convert input to string

    :param val: value to be converted
    :returns: converted value
    :rtype: str
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except:
        pass
    if isinstance(val, str):
        return val
    if kwargs.get('convert_inconsistent_dtypes', True):
        if hasattr(val, '__str__'):
            return str(val)
    return kwargs['nan']


def to_int(val, **kwargs):
    """ Convert input to int

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.int64 
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except:
        pass
    if isinstance(val, np.int64) or isinstance(val, int):
        return np.int64(val)
    if kwargs.get('convert_inconsistent_dtypes', True):
        try:
            return np.int64(val)
        except:
            pass
    return kwargs['nan']


def to_float(val, **kwargs):
    """ Convert input to float

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.float64
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except:
        pass
    if isinstance(val, np.float64) or isinstance(val, float):
        return np.float64(val)
    if kwargs.get('convert_inconsistent_dtypes', True):
        try:
            return np.float64(val)
        except:
            pass
    return kwargs['nan']


def to_date_time(x):
    """ Convert input to numpy.datetime64

    :param x: value to be evaluated
    :returns: evaluated value
    :rtype: numpy.datetime64
    """
    return pd.to_datetime(x, errors='coerce')


def bool_to_str(val, **kwargs):
    """ Convert input boolean to str

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: str
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except:
        pass
    if isinstance(val, np.bool_) or isinstance(val, bool):
        return str(val)
    if kwargs.get('convert_inconsistent_dtypes', True):
        if hasattr(val, '__str__'):
            return str(val)
    return kwargs['nan']


def bool_to_int(val):
    """ Convert input boolean to int

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.int64
    """
    try:
        if pd.isnull(val):
            return kwargs['nan']
    except:
        pass
    if isinstance(val, np.bool_) or isinstance(val, bool):
        return np.int64(val)
    if kwargs.get('convert_inconsistent_dtypes', False):
        try:
            return np.int64(val)
        except:
            pass
    return kwargs['nan']


CONF_FUNCS = {str: to_str,
              int: to_int,
              np.int64: to_int,
              bool: bool_to_str,
              np.bool_: bool_to_str,
              float: to_float,
              np.float64: to_float,
              np.datetime64: to_date_time}
