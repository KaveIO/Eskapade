"""Project: Eskapade - A python-based package for data analysis.

Module: helpers

Created: 2017/05/17

Description:
    Helper functions for Eskapade

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade.logger import Logger

logger = Logger()


def obj_repr(obj):
    """Get generic string representation of object."""
    return object.__repr__(obj)


def apply_transform_funcs(obj, trans_funcs, func_args=None, func_kwargs=None):
    """Transform object by applying transformation functions.

    Transformation functions are applied sequentially to the output of the
    previous function, starting with the specified object.  The final
    resulting object is returned.

    Functions are specified with the "trans_funcs" argument.  This is an
    iterable of either the functions themselves or (function, positional
    arguments, keyword arguments) combinations.  The function arguments can
    also be specified by the "func_args" and "func_kwargs" arguments.  The
    functions are called with the object to be transformed as a first
    argument, followed by the specified positional and keyword arguments.

    A specified function can be either a callable object or a string.  The
    latter is interpreted as a method of the type of the object on which the
    function is applied.

    :param obj: object to transform
    :param iterable trans_funcs: functions to apply, specified the function or a (function, args, kwargs) tuple
    :param dict func_args: function positional arguments, specified as (function, arguments tuple) pairs
    :param dict func_kwargs: function keyword arguments, specified as (function, kwargs dict) pairs
    :returns: transformed object
    """
    # process input functions
    trans_funcs = process_transform_funcs(trans_funcs, func_args, func_kwargs)

    # loop over specified functions
    for func, args, kwargs in trans_funcs:
        # get member function of object type if string is specified
        if isinstance(func, str):
            # get function
            try:
                func = getattr(type(obj), func)
            except AttributeError:
                raise AttributeError('no member function "{0:s}" for object "{1:s}"'.format(func, obj_repr(obj)))

            # check if function is callable
            if not callable(func):
                raise TypeError('function "{0!s}" for object "{1:s}" not callable'.format(func, obj_repr(obj)))

        # apply function
        logger.debug('Applying transformation function "{func!s}" to "{object}":', func=func, object=obj_repr(obj))
        logger.debug('    args:   {args!s}', args=args)
        logger.debug('    kwargs: {kwargs!s}', kwargs=kwargs)
        obj = func(obj, *args, **kwargs)

    return obj


def process_transform_funcs(trans_funcs, func_args=None, func_kwargs=None):
    """Process input of the apply_transform_funcs function.

    :param iterable trans_funcs: functions to apply, specified the function or a (function, args, kwargs) tuple
    :param dict func_args: function positional arguments, specified as (function, arguments tuple) pairs
    :param dict func_kwargs: function keyword arguments, specified as (function, kwargs dict) pairs
    :returns: transformation functions for apply_transform_funcs function
    :rtype: list
    """
    # copy function arguments and make sure they are in dictionaries
    func_args = dict(func_args) if func_args else {}
    func_kwargs = dict(func_kwargs) if func_kwargs else {}

    # loop over specified functions
    proc_funcs = []
    for func_spec in trans_funcs:
        # expand (func, args, kwargs) combinations
        try:
            func, args, kwargs = (func_spec, None, None) if isinstance(func_spec, str) else func_spec
        except TypeError:
            func, args, kwargs = func_spec, None, None
        except ValueError:
            raise ValueError('expected (func, args, kwargs) combination (got {!s})'.format(func_spec))

        # get positional arguments
        args_ = func_args.pop(func, None)
        if args is not None and args_ is not None:
            raise RuntimeError('arguments for "{!s}" in both "trans_funcs" and "func_args"'.format(func))
        args = tuple(args) if args is not None else tuple(args_) if args_ is not None else ()

        # get keyword arguments
        kwargs_ = func_kwargs.pop(func, None)
        if kwargs is not None and kwargs_ is not None:
            raise RuntimeError('keyword arguments for "{!s}" in both "trans_funcs" and "func_kwargs"'.format(func))
        kwargs = dict(kwargs) if kwargs is not None else dict(kwargs_) if kwargs_ is not None else {}

        # check if function is callable
        if not (isinstance(func, str) or callable(func)):
            raise TypeError('function "{!s}" is not callable'.format(func))

        # append function specification
        proc_funcs.append((func, args, kwargs))

    # check if all specified arguments were used
    if func_args:
        raise ValueError('unused function arguments specified: {!s}'.format(func_args))
    if func_kwargs:
        raise ValueError('unused function keyword arguments specified: {!s}'.format(func_kwargs))

    return proc_funcs
