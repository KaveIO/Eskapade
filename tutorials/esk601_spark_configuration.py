# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Macro  : esk601_spark_configuration                                          *
# * Created: 2017/05/31                                                          *
# * Description:                                                                 *
# *     Tutorial macro for configuring Spark in multiple ways                    *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import logging
import os
log = logging.getLogger('macro.esk601_spark_configuration')

import pyspark

from eskapade import ConfigObject, ProcessManager
from analytics_engine.spark_analysis import SparkManager
from analytics_engine import spark_analysis

log.debug('Now parsing configuration file esk601_spark_configuration')


##########################################################################
# --- logging in Spark
#
# 1) through log4j
#    in $SPARK_HOME/conf/log4j.properties set:
#    log4j.logger.org.apache.spark.api.python.PythonGatewayServer=INFO
#
# 2) through SparkContext
#    in Python code set:
#    ProcessManager().service(SparkManager).get_session().sparkContext.setLogLevel('INFO')
#
# NB: get a list of loggers through logging.Logger.manager.loggerDict
#
# logging.getLogger('py4j').setLevel('INFO')
# logging.getLogger('py4j.java_gateway').setLevel('INFO')


##########################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk601_spark_configuration'
settings['version'] = 0


##########################################################################
# --- get Spark Manager to start/stop Spark
sm = proc_mgr.service(SparkManager)


##########################################################################
# --- METHOD 1: configuration file

spark = sm.create_session(eskapade_settings=settings)
sc = spark.sparkContext

log.info('---> METHOD 1: configuration file')
log.info(sc.getConf().getAll())


##########################################################################
# --- METHOD 2: link

conf_link = spark_analysis.SparkConfigurator(name='SparkConfigurator', log_level='WARN')
conf_link.spark_settings = [('spark.app.name', settings['analysisName'] + '_link'),
                            ('spark.master', 'local[42]'),
                            ('spark.driver.host', '127.0.0.1')]
proc_mgr.add_chain('Config').add_link(conf_link)

log.info('---> METHOD 2: link')
log.info('NB: settings will be printed at time of link execution')


##########################################################################
# --- running spark session will be stopped automatically at end


###########################################################################
# --- the end

log.debug('Done parsing configuration file esk601_spark_configuration')
