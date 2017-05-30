# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Module: helpers
# * Created: 2017/05/17                                                          *
# * Description:                                                                 *
# *     Helper functions for Eskapade                                            *
# *                                                                              *
# * Authors:                                                                     *
# *      KPMG Big Data team, Amstelveen, The Netherlands                         *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import logging
import collections

log = logging.getLogger(__name__)


def apply_transform_funcs(obj, trans_funcs):
    """Transform object by applying transformation functions

    :param obj: object to transform
    :param iterable trans_funcs: (function, keyword argument) tuples to apply;
                                 apply member function if function is specified by string
    """

    for func, kwargs in trans_funcs:
        func = getattr(type(obj), func, None) if isinstance(func, str) else func
        if not isinstance(func, collections.Callable):
            log.critical('Transformation function "%s" for object "%s" is not callable', str(func), str(obj))
            raise AssertionError('transformation function must be callable')
        log.debug('Applying transformation function "%s" to "%s" with arguments %s', str(func), str(obj), str(kwargs))
        obj = func(obj, **kwargs)
    return obj
