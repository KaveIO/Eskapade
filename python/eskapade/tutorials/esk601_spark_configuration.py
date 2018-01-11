"""Project: Eskapade - A python-based package for data analysis.

Macro: esk601_spark_configuration

Created: 2017/05/31

Description:
    Tutorial macro for configuring Spark in multiple ways

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, spark_analysis, Chain
from eskapade.logger import Logger
from eskapade.spark_analysis import SparkManager

logger = Logger()

logger.debug('Now parsing configuration file esk601_spark_configuration.')

##########################################################################
# --- logging in Spark
#
# 1) through log4j
#    in $SPARK_HOME/conf/log4j.properties set:
#    log4j.logger.org.apache.spark.api.python.PythonGatewayServer=INFO
#
# 2) through SparkContext
#    in Python code set:
#    process_manager.service(SparkManager).get_session().sparkContext.setLogLevel('INFO')
#
# NB: get a list of loggers through logging.Logger.manager.loggerDict
#
# logging.getLogger('py4j').setLevel('INFO')
# logging.getLogger('py4j.java_gateway').setLevel('INFO')


##########################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk601_spark_configuration'
settings['version'] = 0

##########################################################################
# --- get Spark Manager to start/stop Spark
sm = process_manager.service(SparkManager)

##########################################################################
# --- METHOD 1: configuration file

spark = sm.create_session(eskapade_settings=settings)
sc = spark.sparkContext

logger.info('---> METHOD 1: configuration file')
logger.info(str(sc.getConf().getAll()))

##########################################################################
# --- METHOD 2: link

conf_link = spark_analysis.SparkConfigurator(name='SparkConfigurator', log_level='WARN')
conf_link.spark_settings = [('spark.app.name', settings['analysisName'] + '_link'),
                            ('spark.master', 'local[42]'),
                            ('spark.driver.host', '127.0.0.1')]

config = Chain('Config')
config.add(conf_link)

logger.info('---> METHOD 2: link')
logger.info('NB: settings will be printed at time of link execution.')

##########################################################################
# --- running spark session will be stopped automatically at end


###########################################################################
# --- the end

logger.debug('Done parsing configuration file esk601_spark_configuration.')
