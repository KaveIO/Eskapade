"""Project: Eskapade - A python-based package for data analysis.

Class: SparkConfigurator

Created: 2017/06/07

Description:
    This link stops a running Spark session and starts a new one with
    the configuration provided to the link.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, Link, StatusCode
from eskapade.spark_analysis import SparkManager


class SparkConfigurator(Link):
    """Set configuration settings of SparkContext."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param iterable spark_settings: list of key/value pairs specifying the Spark configuration
        :param str log_level: verbosity level of the SparkContext
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'SparkConfigurator'))

        # process arguments
        self._process_kwargs(kwargs, spark_settings=None, log_level='WARN')
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(log_level=str)
        self.check_arg_iters('spark_settings', allow_none=True)
        self.check_arg_vals('log_level')
        self.spark_settings = list(self.spark_settings) if self.spark_settings else None

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        settings = process_manager.service(ConfigObject)
        sm = process_manager.service(SparkManager)

        # stop running spark session, if any
        sm.finish()

        # start a new session
        spark = process_manager.service(SparkManager).create_session(
            eskapade_settings=settings, spark_settings=self.spark_settings)
        spark.sparkContext.setLogLevel(self.log_level)

        # check config
        self.logger.info('New Spark session started with config: {config!s}',
                         config=spark.sparkContext.getConf().getAll())

        return StatusCode.Success
