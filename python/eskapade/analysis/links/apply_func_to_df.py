"""Project: Eskapade - A python-based package for data analysis.

Class: ApplyFuncToDf

Created: 2016/11/08

Description:
    Algorithm to apply one or more functions to a (grouped) dataframe column
    or to an entire dataframe.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import collections

from eskapade import process_manager, DataStore, Link, StatusCode


class ApplyFuncToDf(Link):
    """Apply functions to data-frame.

    Applies one or more functions to a (grouped) dataframe column or an
    entire dataframe.  In the latter case, this can be done row wise or
    column wise.  The input dataframe will be overwritten.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

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
        """Initialize the link."""
        self.check_arg_vals('read_key')
        if not self.apply_funcs:
            self.logger.warning('No functions to apply')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)
        assert self.read_key in ds, 'key "{key}" not in DataStore.'.format(key=self.read_key)
        df = ds[self.read_key]

        for arr in self.apply_funcs:
            # get func input
            keys = list(arr.keys())
            assert 'func' in keys, 'function input is insufficient.'
            func = arr['func']
            self.logger.debug('Applying function {function!s}.', function=func)
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
            elif 'store_key' in keys:
                if 'entire' in keys:
                    result = func(df, *args, **kwargs)
                elif 'colin' in keys:
                    colin = arr['colin']
                    assert colin in df.columns
                    result = df[colin].apply(func, args=args, **kwargs)
                else:
                    result = df.apply(func, args=args, **kwargs)
                ds[arr['store_key']] = result
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

    def add_apply_func(self, func, out_column, in_column='', *args, **kwargs):
        """Add function to be applied to dataframe."""
        # check inputs
        if not isinstance(func, collections.Callable):
            self.logger.fatal('Specified function object is not callable.')
            raise AssertionError('functions in ApplyFuncToDf link must be callable objects')
        if not isinstance(out_column, str) or not isinstance(in_column, str):
            self.logger.fatal('Types of specified column names are "{in_type}" and "{out_type}."',
                              in_type=type(out_column).__name__, out_type=type(in_column).__name__)
            raise TypeError('Column names in ApplyFuncToDf must be strings.')
        if not out_column:
            self.logger.fatal('No output column specified.')
            raise RuntimeError('An output column must be specified to apply function in ApplyFuncToDf.')

        # add function
        if in_column == '':
            self.apply_funcs.append({'func': func, 'colout': out_column, 'args': args, 'kwargs': kwargs})
        else:
            self.apply_funcs.append({'colin': in_column, 'func': func, 'colout': out_column, 'args': args,
                                     'kwargs': kwargs})

    def groupbyapply(self, df, groupby_columns, applyfunc, *args, **kwargs):
        """Apply groupby to dataframe."""
        if 'groupbyColout' not in kwargs:
            return df.groupby(groupby_columns).apply(applyfunc, *args, **kwargs).reset_index(drop=True)
        else:
            colout = kwargs['groupbyColout']
            kwargs.pop('groupbyColout')
            t = df.groupby(groupby_columns).apply(applyfunc, *args, **kwargs)
            for _ in range(0, len(groupby_columns)):
                t.index = t.index.droplevel()
            df[colout] = t
            return df
