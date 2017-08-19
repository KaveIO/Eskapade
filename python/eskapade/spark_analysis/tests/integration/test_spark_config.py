import unittest
import pyspark

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import eskapade
from eskapade import ConfigObject, ProcessManager
from eskapade.data_quality import dq_helper
from eskapade.tests.integration.test_bases import IntegrationTest
from ...spark_manager import SparkManager


class SparkConfigTest(IntegrationTest):
    """Tests of Spark-configuration"""

    def test_spark_setup(self):
        """Test if Spark setup is working properly"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        settings['analysisName'] = 'spark_setup'

        sm = proc_mgr.service(SparkManager)
        spark = sm.create_session(eskapade_settings=settings)

        df = spark.createDataFrame([(0, 'foo'), (1, 'bar')], ['id', 'value'])

        self.assertSetEqual(set(tuple(r) for r in df.collect()),
                            set([(0, 'foo'), (1, 'bar')]),
                            'unexpected values in columns')
        sm.finish()

    def test_udf_functionality(self):
        """Test if Spark setup is working properly for user-defined functions"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        settings['analysisName'] = 'spark_setup'

        sm = proc_mgr.service(SparkManager)
        spark = sm.create_session(includeEskapadeModules=True, eskapade_settings=settings)

        df = spark.createDataFrame([(0, 'foo'), (1, 'bar')], ['id', 'value'])

        udf_to_str = udf(dq_helper.to_str, StringType())
        df = df.withColumn('output', udf_to_str(df['value']))

        self.assertSetEqual(set(tuple(r) for r in df.collect()),
                            set([(0, 'foo', 'foo'), (1, 'bar', 'bar')]),
                            'unexpected values in columns')
        sm.finish()

    def test_configuring_spark(self):
        """Test configuration of Spark session

        Test setting configuration variables in SparkManager before creating a
        SparkSession.  Configuration with environment variables is not tested
        here, because the unit-test framework and the command line behave
        differently.  Configuration with the SparkConfigurator link is tested in
        the SparkAnalysisTutorialMacrosTest (tutorial esk601).
        """

        sm = ProcessManager().service(SparkManager)

        # create SparkSession
        spark_settings = [('spark.app.name', 'my_spark_session'),
                          ('spark.master', 'local[42]'),
                          ('spark.driver.host', '127.0.0.1')]
        spark = sm.create_session(spark_settings=spark_settings)
        sc = spark.sparkContext

        self.assertEqual(sc.getConf().get('spark.app.name'), 'my_spark_session', 'app name not set correctly')
        self.assertEqual(sc.getConf().get('spark.master'), 'local[42]', 'master not set correctly')
        self.assertEqual(sc.getConf().get('spark.driver.host'), '127.0.0.1', 'driver host not set correctly')

        sm.finish()

        # create new session with different settings - new settings should be picked up
        spark_settings = [('spark.app.name', 'second_spark_session'),
                          ('spark.master', 'local[*]'),
                          ('spark.driver.host', 'localhost')]
        spark = sm.create_session(spark_settings=spark_settings)
        sc = spark.sparkContext

        self.assertEqual(sc.getConf().get('spark.app.name'), 'second_spark_session', 'app name not set correctly')
        self.assertEqual(sc.getConf().get('spark.master'), 'local[*]', 'master not set correctly')
        self.assertEqual(sc.getConf().get('spark.driver.host'), 'localhost', 'driver host not set correctly')

        # specify new settings for already running session - nothing should change
        spark_settings = [('spark.app.name', 'third_spark_session'),
                          ('spark.master', 'local[-1]'),
                          ('spark.driver.host', 'foobar')]
        spark = sm.create_session(spark_settings=spark_settings)
        sc = spark.sparkContext

        self.assertEqual(sc.getConf().get('spark.app.name'), 'second_spark_session', 'app name not set correctly')
        self.assertEqual(sc.getConf().get('spark.master'), 'local[*]', 'master not set correctly')
        self.assertEqual(sc.getConf().get('spark.driver.host'), 'localhost', 'driver host not set correctly')

        sm.finish()
