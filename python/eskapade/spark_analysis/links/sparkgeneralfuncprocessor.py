"""Project: Eskapade - A python-based package for data analysis.

Class: SparkGeneralFuncProcessor

Created: 2016/11/08

Description:
    Processor for applying pandas function on a Spark dataframe.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
from pyspark.sql import DataFrame

from eskapade import process_manager, StatusCode, DataStore, Link


class SparkGeneralFuncProcessor(Link):
    """Processor for applying pandas function on a Spark dataframe.

    The spark API is not (yet) as rich as the pandas API. Therefore sometimes one needs pandas to implement the
    desired algorithm. This link defines a general approach for applying an advanced function using pandas on a
    Spark dataframe. The Spark dataframe is grouped and the general function is applied on each group in parallel.
    In the general function a pandas dataframe can be created as follows:
    pandas_df = pd.DataFrame(list(group), columns=cols)
    For examples, see the function in the deutils.analysishelper module

    This Link uses pyspark.RDD.groupByKey() function instead of pyspark.RDD.reduceBeKey() because one needs all the
    data of one group on one datanode in order to make a pandas dataframe from the group.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        Store the configuration of link SparkToGeneralFuncProcessor.

        :param str name: name of link
        :param str read_key: key of data to read from data store. It should contain a spark dataframe or spark rdd.
        :param str store_key: key of data to store in data store
        :param list groupby: spark dataframe columns to group by
        :param list columns: The columns of the spark dataframe or RDD. Obligatory for RDD, not for spark dataframe.
        :param func generalfunc: The general function. Should be defined by the user. Arguments should be list of
            tuples (rows of RDD), column names and if necessary keyword arguments. Should return a list of native
            python types.
        :param dict function_args: Keyword arguments for the function
        :param int nb_partitions: The number of partitions for repartitioning after groupByKey
        :param func return_map: Function used by the map on the RDD after the generalfunc is applied. The default return
            a tuple of the groupby columns (row[0]) and the list returned by the generalfunc (row[1]).
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'SparkToGeneralFuncProcessor'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', store_key='', groupby=[], columns=None, generalfunc=None,
                             function_args={}, nb_partitions=1200, return_map=lambda row: tuple(list(row[0]) + row[1]))
        # check residual kwargs.
        # (turn line off if you wish to keep these to pass on.)
        self.check_extra_kwargs(kwargs)
        # self.kwargs = kwargs

    def initialize(self):
        """Initialize the link."""
        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        spark_df = ds[self.read_key]

        if isinstance(spark_df, DataFrame):
            if not self.columns:
                self.columns = spark_df.columns
            spark_df = spark_df.rdd
        else:
            if not self.columns:
                self.logger.fatal('Columns are not specified for RDD.')
                raise RuntimeError('Columns are not specified for RDD.')

        res = spark_df.map(lambda row: (tuple([row[c] for c in self.groupby]), row)).groupByKey()\
                      .repartition(self.nb_partitions).mapValues(lambda group: self.generalfunc(group, self.columns,
                                                                                                **self.function_args))\
                      .map(self.return_map)
        ds[self.store_key] = res

        return StatusCode.Success
