"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/05/27

Description:
    Helper functions for the root_analysis links

Authors:
    KPMG Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""


def get_variable_value(values, c, idx, default):
    """Determine value for variables.

    :param values: list of values for variables
    :param list c: list of variables, or string variable
    :param int idx: index of the variable in c for which to return value
    :param default: default return value
    :return: value
    """
    if isinstance(c, str):
        c = [c]
    n = ':'.join(c)
    if n in values and len(values[n]) == len(c) > 1:
        result = values[n][idx]
    else:
        result = values.get(c[idx], default)
    return result
