"""Project: Eskapade - A python-based package for data analysis.

Class: SparkWithColumn

Created: 2017/06/20

Description:
    SparkWithColumn applies a (user-defined) function to column(s) in
    a Spark dataframe and adds its output as a new column to the
    same dataframe.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""


from eskapade import process_manager, DataStore, Link, StatusCode


class SparkWithColumn(Link):
    """Create a new column from columns in a Spark dataframe.

    SparkWithColumn applies a (user-defined) function to column(s) in a
    Spark dataframe and adds its output as a new column to the same
    dataframe.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of data to read from data store
        :param str store_key: key of data to store in data store
        :param str new_column: name of newly created column
        :param col func: a Column expression - function to create the new column with
        :param list col_select: list of column names or columns in a dataframe; specifies to which columns
                                the function is applied, together with col_usage (use all columns if not specified)
        :param list col_usage: use columns in col_select ('include') or any other columns in dataframe ('exclude')
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'SparkWithColumn'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', store_key='', new_column='', func=None, col_select=None,
                             col_usage='include')
        self.check_extra_kwargs(kwargs)

        # initialize other attributes
        self.schema = None

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, store_key=str, col_usage=str, new_column=str)
        self.check_arg_vals('read_key', 'new_column')
        self.check_arg_opts(col_usage=('include', 'exclude'))
        self.check_arg_iters('col_select', allow_none=True)
        self.check_arg_callable('func')
        if not self.store_key:
            self.store_key = self.read_key
        if self.col_select is not None:
            self.col_select = tuple(self.col_select)

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # fetch data frame
        ds = process_manager.service(DataStore)
        spark_df = ds[self.read_key]

        # use all columns if columns-select argument was not provided
        if self.col_select is None:
            self.col_select = tuple(spark_df.columns)

        # set list of columns to apply the function to
        # As the columns in 'col_select' can be either column names (strings) or the actual columns, some additional
        # processing is required in the below.
        cols = [spark_df[c] if isinstance(c, str) else c for c in self.col_select]
        if self.col_usage == 'exclude':
            # apply function to all columns in the dataframe which are not in 'col_select'
            col_names = [str(c) for c in cols]
            cols = [spark_df[c] for c in spark_df.columns if c not in col_names]

        # apply function
        new_spark_df = spark_df.withColumn(self.new_column, self.func(*cols))

        # store updated data frame
        ds[self.store_key] = new_spark_df

        return StatusCode.Success
