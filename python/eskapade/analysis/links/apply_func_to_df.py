# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : ApplyFuncToDf                                                  *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to apply one or more functions to a (grouped) dataframe column  *
# *      or to an entire dataframe.                                                *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import collections

from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class ApplyFuncToDf(Link):
    """Apply functions to data-frame

    Applies one or more functions to a (grouped) dataframe column or an
    entire dataframe.  In the latter case, this can be done row wise or
    column wise.  The input dataframe will be overwritten.
    """

    def __init__(self, **kwargs):
        """Initialize link instance

        :param str read_key: data-store input key
        :param str store_key: data-store output key
        :param list apply_funcs: functions to apply (list of dicts)
          - 'func': function to apply
          - 'colout' (string): output column
          - 'colin' (string, optional): input column
          - 'entire' (boolean, optional): apply to the entire dataframe?
          - 'args' (tuple, optional): args for 'func'
          - 'kwargs' (dict, optional): kwargs for 'func'
          - 'groupby' (list, optional): column names to group by
          - 'groupbyColout' (string) output column after the split-apply-combine combination
        :param dict add_columns: columns to add to output (name, column)
        """

        Link.__init__(self, kwargs.pop('name', 'apply_func_to_dataframe'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', store_key='', apply_funcs=[], add_columns=None)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize link"""

        self.check_arg_vals('read_key')
        if not self.apply_funcs:
            self.log().warning('No functions to apply')

        return StatusCode.Success

    def execute(self):
        """Execute link"""

        ds = process_manager.service(DataStore)
        assert self.read_key in list(ds.keys()), 'key <%s> not in DataStore.' % self.read_key
        df = ds[self.read_key]

        for arr in self.apply_funcs:
            # get func input
            keys = list(arr.keys())
            assert 'func' in keys, 'function input is insufficient.'
            func = arr['func']
            self.log().debug('Applying function %s' % str(func))
            args = ()
            kwargs = {}
            if 'kwargs' in keys:
                kwargs = arr['kwargs']
            if 'args' in keys:
                args = arr['args']

            # apply func
            if 'groupby' in keys:
                groupby = arr['groupby']
                if 'groupbyColout' in keys:
                    kwargs['groupbyColout'] = arr['groupbyColout']
                df = self.groupbyapply(df, groupby, func, *args, **kwargs)
            elif 'storekey' in keys:
                if 'entire' in keys:
                    result = func(df, *args, **kwargs)
                elif 'colin' in keys:
                    colin = arr['colin']
                    assert colin in df.columns
                    result = df[colin].apply(func, args=args, **kwargs)
                else:
                    result = df.apply(func, args=args, **kwargs)
                ds[arr['storekey']] = result
            else:
                assert 'colout' in keys, 'function input is insufficient'
                colout = arr['colout']
                if 'entire' in keys:
                    df[colout] = func(df, *args, **kwargs)
                elif 'colin' in keys:
                    colin = arr['colin']
                    if isinstance(colin, list):
                        for c in colin:
                            assert c in df.columns
                    else:
                        assert colin in df.columns
                    df[colout] = df[colin].apply(func, args=args, **kwargs)
                else:
                    df[colout] = df.apply(func, args=args, **kwargs)

        # add columns
        if self.add_columns is not None:
            for k, v in self.add_columns.items():
                df[k] = v

        if self.store_key is None:
            ds[self.read_key] = df
        else:
            ds[self.store_key] = df

        return StatusCode.Success

    def addApplyFunc(self, func, outColumn, inColumn='', *args, **kwargs):
        """Add function to be applied to dataframe"""

        # check inputs
        if not isinstance(func, collections.Callable):
            self.log().critical('specified function object is not callable')
            raise AssertionError('functions in ApplyFuncToDf link must be callable objects')
        if not isinstance(outColumn, str) or not isinstance(inColumn, str):
            self.log().critical('types of specified column names are "%s" and "%s"',
                                type(outColumn).__name__, type(inColumn).__name__)
            raise TypeError('column names in ApplyFuncToDf must be strings')
        if not outColumn:
            self.log().critical('no output column specified')
            raise RuntimeError('an output column must be specified to apply function in ApplyFuncToDf')

        # add function
        if inColumn == '':
            self.apply_funcs.append({'func': func, 'colout': outColumn, 'args': args, 'kwargs': kwargs})
        else:
            self.apply_funcs.append({'colin': inColumn, 'func': func, 'colout': outColumn, 'args': args,
                                     'kwargs': kwargs})

    def groupbyapply(self, df, groupbyColumns, applyfunc, *args, **kwargs):
        """Apply groupby to dataframe"""

        if 'groupbyColout' not in list(kwargs.keys()):
            return df.groupby(groupbyColumns).apply(applyfunc, *args, **kwargs).reset_index(drop=True)
        else:
            colout = kwargs['groupbyColout']
            kwargs.pop('groupbyColout')
            t = df.groupby(groupbyColumns).apply(applyfunc, *args, **kwargs)
            for i in range(0, len(groupbyColumns)):
                t.index = t.index.droplevel()
            df[colout] = t
            return df
