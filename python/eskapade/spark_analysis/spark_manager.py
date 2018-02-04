"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/02/27

Class: SparkManager

Description:
    Process service for managing Spark operations

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import logging
import os

import pyspark

import eskapade
from eskapade import resources
from eskapade.core import persistence
from eskapade.core.mixin import ConfigMixin
from eskapade.core.process_services import ProcessService
from eskapade.spark_analysis.functions import SPARK_UDFS

logging.getLogger('py4j.java_gateway').setLevel('INFO')

CONF_PREFIX = 'spark'


class SparkManager(ProcessService, ConfigMixin):
    """Process service for managing Spark operations."""

    _persist = False

    def __init__(self, config_path=None):
        """Initialize Spark manager instance."""
        self._session = None
        self._stream = None
        ConfigMixin.__init__(self, config_path=config_path)

    def create_session(self, enable_hive_support=False, include_eskapade_modules=False, **conf_kwargs):
        """Get or create Spark session.

        Return the Spark-session instance.  Create the session if it does not
        exist yet.  If no SparkConfig is set yet, it is created.  All keyword
        arguments are passed to the _create_spark_conf method in this case.

        :param bool enable_hive_support: switch for enabling Spark Hive support
        :param bool include_eskapade_modules: switch to include Eskapade modules in Spark job submission.
            Default is False. Optional.
        """
        # return existing session if still running
        if self._session and self._session.sparkContext._jsc:
            self.logger.debug('Spark session already running, returning existing instance.')
            if conf_kwargs:
                self.logger.warning('Spark session already running, ignoring new settings.')
            return self._session

        # complain if there is no running spark context
        if self._session and not self._session.sparkContext._jsc:
            self.logger.warning('Spark session exists but no running context available, will restart.')

        # set Spark configuration parameters
        spark_conf = self._create_spark_conf(**conf_kwargs)

        # include Eskapade modules in Spark job submission
        if include_eskapade_modules:
            py_mods = eskapade.utils.collect_python_modules()
            for key in ['spark.submit.pyFiles', 'spark.files']:
                files = spark_conf.get(key, '')
                if py_mods not in files:
                    spark_conf.set(key, ','.join(c for c in (py_mods, files) if c))

        # create Spark session
        spark_builder = pyspark.sql.SparkSession.builder
        if enable_hive_support:
            spark_builder = spark_builder.enableHiveSupport()
        self._session = spark_builder.config(conf=spark_conf).getOrCreate()

        # register user-defined functions
        for udf_name, udf in SPARK_UDFS.items():
            ret_type = getattr(pyspark.sql.types, udf.get('ret_type', 'StringType'))()
            self._session.udf.register(name=udf_name, f=udf['func'], returnType=ret_type)

        self.logger.debug('Created Spark session with config {config!s}.',
                          config=self._session.sparkContext.getConf().getAll())

        return self._session

    def get_session(self):
        """Get Spark session.

        Return running Spark session and check if the Spark context is still alive.
        """
        # return existing session if still running
        if not self._session:
            raise RuntimeError('No running Spark session available, please start one first.')

        # return existing session if still running
        if self._session and not self._session.sparkContext._jsc:
            self.logger.warning('Spark context not running, consider creating a new Spark session.')

        return self._session

    def _create_spark_conf(self, eskapade_settings=None, config_path=None, spark_settings=None):
        """Create and set Spark configuration.

        Read the Spark configuration file and store the settings as a SparkConf
        object.  The path of the configuration file is given by the config_path
        argument or, if this argument is not specified, it is obtained from the
        Eskapade settings object (key "sparkCfgFile").  If neither of these
        inputs are provided, an empty configuration object is created.

        With the spark_settings argument, settings from the configuration file
        can be overwritten.  Also additional settings can be specified with this
        argument.

        :param eskapade.ConfigObject eskapade_settings: Eskapade configuration (key "sparkCfgFile" for config path)
        :param str config_path: path of configuration file
        :param iterable spark_settings: iterable of custom settings key-value pairs to be set
        """
        # set path of config file
        cfg_path = str(config_path) if config_path else str(eskapade_settings.get('sparkCfgFile')) \
            if eskapade_settings and eskapade_settings.get('sparkCfgFile') else None
        if cfg_path and eskapade_settings and not os.path.isabs(cfg_path):
            cfg_path = resources.config(cfg_path)
        if cfg_path and cfg_path != self.config_path:
            self.logger.debug('Setting configuration file path to "{path}".', path=cfg_path)
            self.config_path = cfg_path
            self.reset_config()

        # create Spark config
        spark_conf = pyspark.conf.SparkConf()

        # set settings from config file
        if self.config_path:
            cfg = self.get_config()
            if CONF_PREFIX in cfg:
                spark_conf.setAll(cfg.items(CONF_PREFIX))
            elif not spark_settings:
                raise RuntimeError('No section "{}" found in config file.'.format(CONF_PREFIX))

        # set custom settings
        if spark_settings:
            spark_conf.setAll(spark_settings)

        return spark_conf

    @property
    def spark_streaming_context(self):
        """Spark Streaming Context."""
        return self._stream

    @spark_streaming_context.setter
    def spark_streaming_context(self, ssc):
        """Set Spark Streaming Context."""
        if self._stream:
            raise RuntimeError('Cannot set Spark Streaming Context on top of an existing Spark Streaming Context.')

        if not isinstance(ssc, pyspark.streaming.StreamingContext):
            self.logger.fatal(
                'Specified streaming context object is not a pyspark.streaming.StreamingContext (got "{type}")',
                type=type(ssc).__name__)
            raise TypeError('Incorrect type for Spark Streaming Context object.')

        self._stream = ssc
        self.logger.debug('Set Spark Streaming Context.')

    def finish(self):
        """Stop Spark session."""
        if self._stream:
            self.logger.debug('Spark manager stopping streaming context.')
            self._stream.stop()
            self._stream = None

        if self._session:
            self.logger.debug('Spark manager stopping session: "{app_name}"',
                              app_name=self._session.sparkContext.appName)
            self._session.stop()
            self._session = None
