"""Project: Eskapade - A python-based package for data analysis.

Module: spark_analysis.functions

Created: 2017/05/24

Description:
    Collection of Spark functions defined for Eskapade

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
import pandas as pd

import pyspark


def is_nan(x):
    """Test if value is NaN/null/None."""
    if pd.isnull(x):
        return True
    if isinstance(x, str):
        x = x.strip().lower()
        if not x or x == 'none' or x == 'nan':
            return True
    return False


def is_inf(x):
    """Test if value is infinite."""
    try:
        return bool(np.isinf(x))
    except BaseException:
        return False


def to_date_time(dt, tz_in=None, tz_out=None):
    """Convert value to date/time object.

    :param dt: value representing a date/time (parsed by pandas.Timestamp)
    :param tz_in: time zone to localize data/time value to (parsed by pandas.Timestamp.tz_localize)
    :param tz_out: time zone to convert data/time value into (parsed by pandas.Timestamp.tz_convert)
    :returns: date/time object
    :rtype: datetime.datetime
    """
    if dt is None:
        return None
    try:
        ts = pd.Timestamp(dt)
        if tz_in:
            ts = ts.tz_localize(tz_in)
        if tz_out:
            ts = ts.tz_convert(tz_out)
        return ts.to_datetime()
    except BaseException:
        return None


def to_timestamp(dt, tz_in=None):
    """Convert value to Unix timestamp (ns).

    :param dt: value representing a date/time (parsed by pandas.Timestamp)
    :param tz_in: time zone to localize data/time value to (parsed by pandas.Timestamp.tz_localize)
    :returns: Unix timestamp (ns)
    :rtype: int
    """
    if dt is None:
        return None
    try:
        ts = pd.Timestamp(dt)
        if tz_in:
            ts = ts.tz_localize(tz_in)
        return ts.value
    except BaseException:
        return None


def calc_asym(var1, var2):
    """Calculate asymmetry.

    Calculate asymmetry between variables 1 and 2:
    >>> (var2 - var1) / (abs(var1) + abs(var2))

    :returns: asymmetry value
    :rtype: float
    """
    try:
        var1 = np.float64(var1)
        var2 = np.float64(var2)
    except BaseException:
        return None

    return float((var2 - var1) / (abs(var1) + abs(var2)))


SPARK_UDFS = dict(is_nan=dict(func=is_nan, ret_type='BooleanType'),
                  is_inf=dict(func=is_inf, ret_type='BooleanType'),
                  to_date_time=dict(func=to_date_time, ret_type='TimestampType'),
                  to_timestamp=dict(func=to_timestamp, ret_type='LongType'),
                  calc_asym=dict(func=calc_asym, ret_type='DoubleType'))

_only_finite = "if(is_nan({0:s}) or is_inf({0:s}), NULL, {0:s})"
SPARK_QUERY_FUNCS = dict(count='count(*)',
                         colcount='count({0:s})',
                         nunique='count(distinct {0:s})',
                         nnan='count(if(is_nan({0:s}), 1, NULL))',
                         ninf='count(if(is_inf({0:s}), 1, NULL))',
                         sum='sum({})'.format(_only_finite),
                         possum='sum(if(not is_nan({0:s}) and not is_inf({0:s}) and {0:s}>0, {0:s}, NULL))',
                         negsum='sum(if(not is_nan({0:s}) and not is_inf({0:s}) and {0:s}<0, {0:s}, NULL))',
                         mean='avg({})'.format(_only_finite),
                         max='max({})'.format(_only_finite),
                         min='min({})'.format(_only_finite),
                         std='stddev_pop({})'.format(_only_finite),
                         skew='skewness({})'.format(_only_finite),
                         kurt='kurtosis({})'.format(_only_finite),
                         var='var_pop({})'.format(_only_finite),
                         cov='covar_pop({},{})'.format(_only_finite, _only_finite.format('{1:s}')),
                         corr='corr({},{})'.format(_only_finite, _only_finite.format('{1:s}')),
                         msd='if(is_nan({0:s}) or is_inf({0:s}) or {0:s}=0, NULL, '
                             'cast(abs({0:s}) * pow(10, -floor(log10(abs({0:s})))) as int))')


def spark_sql_func(name, default_func=None):
    """Get Spark SQL function.

    Get a function from pyspark.sql.functions by name.  If function does not
    exist in the SQL-functions module, return a default function, if
    specified.

    :param str name: name of function
    :param default_func: default function
    :returns: Spark SQL function
    :raises: RuntimeError if function does not exist
    """
    func = getattr(pyspark.sql.functions, str(name), default_func)
    if func:
        return func
    raise RuntimeError('"{!s}" not found in Spark SQL functions'.format(name))


def spark_query_func(spec):
    """Get Eskapade Spark-query function.

    Get a function that returns a string to be used as a function in a Spark
    SQL query:

    >>> count_fun = spark_query_func('count')
    >>> count_fun()
    'count(*)'
    >>> cov_fun = spark_query_func('cov')
    >>> cov_fun('x', 'y')
    'covar_pop(if(is_nan(x) or is_inf(x), NULL, x),if(is_nan(y) or is_inf(y), NULL, y))'
    >>> my_fun = spark_query_func('my_func::count(if({0:s} == 0, 1, NULL))')
    >>> my_fun.__name__
    'my_func'
    >>> my_fun('my_var')
    'count(if(my_var == 0, 1, NULL))'

    :param str spec: function specification: "name" or "name::definition"
    :returns: query function
    """
    # split function specification into name and definition
    sep_ind = spec.find('::')
    if sep_ind in (0, len(spec) - 2):
        raise ValueError('invalid function specification: "{!s}"'.format(spec))
    name, func_def = (spec[:sep_ind], spec[sep_ind + 2:]) if sep_ind > 0 else (spec, spec)

    # get function definition
    func_def = SPARK_QUERY_FUNCS.get(str(name), func_def)
    if not func_def:
        raise RuntimeError('no definition found for Spark query function "{}"'.format(name))

    # build function
    def query_func(*args):
        """Build query function."""
        return str(func_def).format(*args)

    query_func.__name__ = str(name)

    return query_func
