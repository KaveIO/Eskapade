import os
import logging

import pyspark

import eskapade
from eskapade import process_manager, ConfigObject
from eskapade.core import persistence
from eskapade.mixins import ConfigMixin
from eskapade.core.process_services import ProcessService
from eskapade.spark_analysis.functions import SPARK_UDFS

logging.getLogger('py4j.java_gateway').setLevel('INFO')

CONF_PREFIX = 'spark'


class SparkManager(ProcessService, ConfigMixin):
    """Process service for managing Spark operations"""

    _persist = False

    def __init__(self, config_path=None):
        """Initialize Spark manager instance"""

        self._session = None
        self._stream = None
        ConfigMixin.__init__(self, config_path=config_path)

    def create_session(self, enableHiveSupport=False, includeEskapadeModules=False, **conf_kwargs):
        """Get or create Spark session

        Return the Spark-session instance.  Create the session if it does not
        exist yet.  If no SparkConfig is set yet, it is created.  All keyword
        arguments are passed to the _create_spark_conf method in this case.

        :param bool enableHiveSupport: switch for enabling Spark Hive support
        :param bool includeEskapadeModules: switch to include Eskapade modules in Spark job submission
        """

        # return existing session if still running
        if self._session and self._session.sparkContext._jsc:
            self.log().debug('Spark session already running, returning existing instance')
            if conf_kwargs:
                self.log().warning('Spark session already running, ignoring new settings')
            return self._session

        # complain if there is no running spark context
        if self._session and not self._session.sparkContext._jsc:
            self.log().warning('Spark session exists but no running context available, will restart')

        # set Spark configuration parameters
        spark_conf = self._create_spark_conf(**conf_kwargs)

        # include Eskapade modules in Spark job submission
        if includeEskapadeModules:
            eskapade.utils.collect_python_modules()
            py_mods = eskapade.utils.get_file_path('py_mods')
            for key in ['spark.submit.pyFiles', 'spark.files']:
                files = spark_conf.get(key, '')
                if not py_mods in files:
                    spark_conf.set(key, ','.join(c for c in (py_mods, files) if c))

        # create Spark session
        spark_builder = pyspark.sql.SparkSession.builder
        if enableHiveSupport:
            spark_builder = spark_builder.enableHiveSupport()
        self._session = spark_builder.config(conf=spark_conf).getOrCreate()

        # register user-defined functions
        for udf_name, udf in SPARK_UDFS.items():
            ret_type = getattr(pyspark.sql.types, udf.get('ret_type', 'StringType'))()
            self._session.udf.register(name=udf_name, f=udf['func'], returnType=ret_type)

        self.log().debug('Created Spark session with config {}'.format(str(self._session.sparkContext.getConf().getAll())))

        return self._session

    def get_session(self):
        """Get Spark session

        Return running Spark session and check if the Spark context is still alive. 
        """

        # return existing session if still running
        if not self._session:
            raise RuntimeError('No running Spark session available, please start one first')

        # return existing session if still running
        if self._session and not self._session.sparkContext._jsc:
            self.log().warning('Spark context not running, consider creating a new Spark session.')

        return self._session

    def _create_spark_conf(self, eskapade_settings=None, config_path=None, spark_settings=None):
        """Create and set Spark configuration
        Read the Spark configuration file and store the settings as a SparkConf
        object.  The path of the configuration file is given by the config_path
        argument or, if this argument is not specified, it is obtained from the
        Eskapade settings object (key "sparkCfgFile").  If neither of these
        inputs are provided, an empty configuration object is created.

        With the spark_settings argument, settings from the configuration file
        can be overwritten.  Also additional settings can be specified with this
        argument.

        :param str config_path: path of configuration file
        :param eskapade.ConfigObject es_settings_obj: Eskapade configuration (key "sparkCfgFile" for config path)
        :param iterable spark_settings: iterable of custom settings key-value pairs to be set
        """

        # set path of config file
        cfg_path = str(config_path) if config_path else str(
            eskapade_settings.get('sparkCfgFile')) if eskapade_settings else None
        if cfg_path and eskapade_settings and not os.path.isabs(cfg_path):
            cfg_path = persistence.io_path('config_spark', eskapade_settings.io_conf(), cfg_path)
        if cfg_path and cfg_path != self.config_path:
            self.log().debug('Setting configuration file path to "{}"'.format(cfg_path))
            self.config_path = cfg_path
            self.reset_config()

        # create Spark config
        spark_conf = pyspark.conf.SparkConf()

        # set settings from config file
        if self.config_path:
            cfg = self.get_config()
            if not CONF_PREFIX in cfg:
                raise RuntimeError('No section "{}" found in config file'.format(CONF_PREFIX))
            spark_conf.setAll(cfg.items(CONF_PREFIX))

        # set custom settings
        if spark_settings:
            spark_conf.setAll(spark_settings)

        return spark_conf

    @property
    def spark_streaming_context(self):
        """Spark Streaming Context"""

        return self._stream

    @spark_streaming_context.setter
    def spark_streaming_context(self, ssc):
        """Set Spark Streaming Context"""

        if self._stream:
            raise RuntimeError('cannot set Spark Streaming Context on top of an existing Spark Streaming Context')

        if not isinstance(ssc, pyspark.streaming.StreamingContext):
            self.log().critical(
                'Specified streaming context object is not a pyspark.streaming.StreamingContext (got "{}")'.format(
                    type(ssc).__name__))
            raise TypeError('incorrect type for Spark Streaming Context object')

        self._stream = ssc
        self.log().debug('Set Spark Streaming Context')

    def finish(self):
        """Stop Spark session"""

        if self._stream:
            self.log().debug('Spark manager stopping streaming context')
            self._stream.stop()
            self._stream = None

        if self._session:
            self.log().debug('Spark manager stopping session: "{}"'.format(self._session.sparkContext.appName))
            self._session.stop()
            self._session = None
