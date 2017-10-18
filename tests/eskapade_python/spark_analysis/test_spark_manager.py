import unittest
import unittest.mock as mock

import pyspark

from eskapade.spark_analysis.functions import SPARK_UDFS
from eskapade.spark_analysis.spark_manager import SparkManager


class SparkManagerTest(unittest.TestCase):
    """Tests for the Spark-manager process service"""

    def test_persist(self):
        """Test persistence flag of Spark manager"""

        self.assertFalse(SparkManager._persist, 'Spark manager is persisting')

    def test_init(self):
        """Test initialization of Spark manager"""

        mock_sm = mock.Mock(name='spark_manager')
        SparkManager.__init__(mock_sm)
        self.assertFalse(bool(mock_sm._session), 'incorrect initialization of session attribute')

    @mock.patch.dict('eskapade.spark_analysis.functions.SPARK_UDFS', clear=True)
    @mock.patch('eskapade.utils')
    @mock.patch('pyspark.sql.types')
    @mock.patch('pyspark.sql.SparkSession.builder')
    def test_create_session(self, mock_builder, mock_types, mock_es_utils):
        """Test creation of Spark session"""

        # create mock sessions
        mock_sm = mock.Mock(name='spark_manager')
        # mock_ss = mock.Mock(name='mock_spark_session')
        mock_sc = mock.Mock(name='mock_spark_context')
        mock_jv = mock.Mock(name='mock_java_context')

        existing_session = mock.Mock('existing_spark_session')
        created_session = mock.Mock(name='created_spark_session')
        created_session.getOrCreate.return_value = created_session
        created_session.config.return_value = created_session

        def conf_get(*args):
            if len(args) < 2:
                return None
            return args[1]
        created_config = mock.Mock(name='created_spark_conf')
        created_config.get.side_effect = conf_get
        mock_builder.config.return_value = created_session
        mock_builder.enableHiveSupport.return_value = created_session
        mock_sm._create_spark_conf.return_value = created_config
        mock_es_utils.collect_python_modules.return_value = 'py_mods_path'

        # create mock UDFs
        SPARK_UDFS['foo'] = dict(func='foo_func', ret_type='foo_type')
        SPARK_UDFS['bar'] = dict(func='bar_func', ret_type='bar_type')

        # test returning of existing session
        mock_sm._session = existing_session
        mock_sm._session.sparkContext = mock_sc
        mock_sm._session.sparkContext._jsc = mock_jv
        session = SparkManager.create_session(mock_sm)
        mock_sm._create_spark_conf.assert_not_called()
        mock_es_utils.collect_python_modules.assert_not_called()
        mock_builder.enableHiveSupport.assert_not_called()
        mock_builder.config.assert_not_called()
        created_session.assert_not_called()
        created_session.getOrCreate.assert_not_called()
        self.assertIs(session, existing_session, 'incorrect session returned')
        self.assertIs(mock_sm._session, existing_session, 'incorrect session set')
        mock_sm.reset_mock()
        mock_es_utils.reset_mock()
        mock_builder.reset_mock()

        # test returning of new session if Java Spark context was stopped
        mock_sm._session = existing_session
        mock_sm._session.sparkContext = mock_sc
        mock_sm._session.sparkContext._jsc = None
        session = SparkManager.create_session(mock_sm)
        mock_sm._create_spark_conf.assert_called_once_with()
        mock_es_utils.collect_python_modules.assert_not_called()
        mock_builder.enableHiveSupport.assert_not_called()
        mock_builder.config.assert_called_once_with(conf=created_config)
        created_session.config.assert_not_called()
        created_session.getOrCreate.assert_called_once_with()
        self.assertIs(session, created_session, 'incorrect session returned')
        self.assertIs(mock_sm._session, created_session, 'incorrect session set')
        mock_sm.reset_mock()
        mock_es_utils.reset_mock()
        mock_builder.reset_mock()
        created_session.reset_mock()

        # test returning of newly created session without Hive support (default)
        mock_sm._session = None
        session = SparkManager.create_session(mock_sm)
        mock_sm._create_spark_conf.assert_called_once_with()
        mock_es_utils.collect_python_modules.assert_not_called()
        mock_builder.enableHiveSupport.assert_not_called()
        mock_builder.config.assert_called_once_with(conf=created_config)
        created_session.config.assert_not_called()
        created_session.getOrCreate.assert_called_once_with()
        self.assertIs(session, created_session, 'incorrect session returned')
        self.assertIs(mock_sm._session, created_session, 'incorrect session set')
        # check UDF calls
        udf_calls = [mock.call.udf.register(name=n, f='{}_func'.format(n),
                                            returnType=getattr(mock_types, '{}_type'.format(n))()) for n in SPARK_UDFS]
        created_session.assert_has_calls(udf_calls, any_order=True)
        mock_sm.reset_mock()
        mock_es_utils.reset_mock()
        mock_builder.reset_mock()
        created_session.reset_mock()

        # test returning of newly created session with Hive support
        mock_sm._session = None
        session = SparkManager.create_session(mock_sm, enable_hive_support=True)
        mock_sm._create_spark_conf.assert_called_once_with()
        mock_es_utils.collect_python_modules.assert_not_called()
        mock_builder.enableHiveSupport.assert_called_once_with()
        mock_builder.config.assert_not_called()
        created_session.config.assert_called_once_with(conf=created_config)
        created_session.getOrCreate.assert_called_once_with()
        self.assertIs(session, created_session, 'incorrect session returned')
        self.assertIs(mock_sm._session, created_session, 'incorrect session set')
        mock_sm.reset_mock()
        mock_es_utils.reset_mock()
        mock_builder.reset_mock()
        created_session.reset_mock()

        # test returning of newly created session with Eskapade modules submission
        mock_sm._session = None
        session = SparkManager.create_session(mock_sm, include_eskapade_modules=True)
        mock_sm._create_spark_conf.assert_called_once_with()
        mock_es_utils.collect_python_modules.assert_called_once_with()
        mock_builder.enableHiveSupport.assert_not_called()
        mock_builder.config.assert_called_once_with(conf=created_config)
        created_config.set.assert_any_call('spark.submit.pyFiles', 'py_mods_path')
        created_config.set.assert_any_call('spark.files', 'py_mods_path')
        created_session.getOrCreate.assert_called_once_with()
        self.assertIs(session, created_session, 'incorrect session returned')
        self.assertIs(mock_sm._session, created_session, 'incorrect session set')
        mock_sm.reset_mock()
        mock_es_utils.reset_mock()
        mock_builder.reset_mock()
        created_session.reset_mock()

    def test_get_session(self):
        """Test retrieving of already running Spark session"""

        # create mock sessions
        mock_sm = mock.Mock(name='spark_manager')
        # mock_ss = mock.Mock(name='mock_spark_session')
        mock_sc = mock.Mock(name='mock_spark_context')
        mock_jv = mock.Mock(name='mock_java_context')

        existing_session = mock.Mock('existing_spark_session')

        # test returning of existing session
        mock_sm._session = existing_session
        mock_sm._session.sparkContext = mock_sc
        mock_sm._session.sparkContext._jsc = mock_jv
        session = SparkManager.get_session(mock_sm)
        self.assertIs(session, existing_session, 'incorrect session returned')
        self.assertIs(mock_sm._session, existing_session, 'incorrect session set')
        mock_sm.reset_mock()

        # test returning of new session if Java Spark context was stopped
        mock_sm._session = existing_session
        mock_sm._session.sparkContext = mock_sc
        mock_sm._session.sparkContext._jsc = None
        session = SparkManager.get_session(mock_sm)
        self.assertIs(session, existing_session, 'incorrect session returned')
        self.assertIs(mock_sm._session, existing_session, 'incorrect session set')
        mock_sm.reset_mock()

        # test returning of new session if Java Spark context was stopped
        mock_sm._session = None
        with self.assertRaises(RuntimeError):
            SparkManager.get_session(mock_sm)
        mock_sm.reset_mock()

    @mock.patch('eskapade.core.persistence.io_path')
    @mock.patch('pyspark.conf')
    def test_create_spark_conf(self, mock_conf_mod, mock_io_path):
        """Test creation of Spark configuration"""

        # create mock-ups
        mock_sm = mock.Mock(name='spark_manager')
        mock_sm.config_path = None
        created_config = mock.MagicMock(name='spark_conf')
        eskapade_settings = mock.Mock(name='eskapade_settings')
        spark_settings = mock.Mock(name='custom_spark_settings')
        mock_conf_mod.SparkConf.return_value = created_config
        mock_io_path.return_value = lambda a, b, c: '/foo/bar'

        # test creating config without settings
        mock_sm.config_path = None
        default_config = SparkManager._create_spark_conf(mock_sm)
        mock_sm.reset_config.assert_not_called()
        created_config.setAll.assert_not_called()
        self.assertIs(default_config, created_config)
        self.assertIs(mock_sm.config_path, None, 'incorrect config path set')
        mock_sm.reset_mock()
        created_config.reset_mock()

        # test creating config with Eskapade settings
        mock_sm.config_path = None

        def get(arg):
            return {'sparkCfgFile': 'spark.cfg'}[arg]

        eskapade_settings.get = get
        eskapade_settings.io_conf.return_value = dict(foo='bar')

        def get_config(*args):
            class MockConf(list):
                def __init__(self, *args):
                    list.__init__(self, *args)
                    self.append('spark')

                def items(self, *args):
                    return [('foo', 'bar')]

            dummy = MockConf()
            return dummy

        mock_sm.get_config.side_effect = get_config
        eskapade_config = SparkManager._create_spark_conf(mock_sm, eskapade_settings=eskapade_settings)
        mock_io_path.assert_called_once_with('config_spark', 'spark.cfg', {'foo': 'bar'})
        mock_sm.reset_config.assert_called_once_with()
        created_config.setAll.assert_called_once_with([('foo', 'bar')])
        self.assertIs(eskapade_config, created_config, 'incorrect config set')
        mock_sm.reset_mock()
        mock_io_path.reset_mock()
        created_config.reset_mock()

        # test creating config with alternative configuration file path
        mock_sm.config_path = None

        mock_sm.get_config.side_effect = get_config
        file_config = SparkManager._create_spark_conf(mock_sm, config_path='/foo/bar')
        mock_io_path.assert_not_called()
        mock_sm.reset_config.assert_called_once_with()
        created_config.setAll.assert_called_once_with([('foo', 'bar')])
        self.assertIs(file_config, created_config, 'incorrect config set')
        mock_sm.reset_mock()
        mock_io_path.reset_mock()
        created_config.reset_mock()

        # test failing of creating config due to absence spark section in configuration file
        mock_sm.config_path = None

        def get_config_wrong(*args):
            class MockConf(list):
                def __init__(self, *args):
                    list.__init__(self, *args)
                    self.append('kraps')

                def items(self, *args):
                    return [('foo', 'bar')]
            dummy = MockConf()
            return dummy

        mock_sm.get_config.side_effect = get_config_wrong
        with self.assertRaises(RuntimeError):
            SparkManager._create_spark_conf(mock_sm, config_path='/foo/bar')
        mock_io_path.assert_not_called()
        mock_sm.reset_config.assert_called_once_with()
        mock_sm.reset_mock()
        mock_io_path.reset_mock()
        created_config.reset_mock()

        # test creating config with custom Spark settings
        mock_sm.config_path = None
        custom_config = SparkManager._create_spark_conf(mock_sm, spark_settings=spark_settings)
        mock_sm.reset_config.assert_not_called()
        created_config.setAll.assert_called_once_with(spark_settings)
        self.assertIs(custom_config, created_config)
        self.assertIs(mock_sm.config_path, None, 'incorrect config path set')
        mock_sm.reset_mock()
        created_config.reset_mock()

        # test creating config with all parameters set
        mock_sm.config_path = None
        eskapade_settings.io_conf.return_value = dict(foo='bar')

        mock_sm.get_config.side_effect = get_config
        SparkManager._create_spark_conf(
            mock_sm, config_path='foo.bar', eskapade_settings=eskapade_settings, spark_settings=spark_settings)
        mock_io_path.assert_called_once_with('config_spark', 'foo.bar', {'foo': 'bar'})
        mock_sm.reset_config.assert_called_once_with()
        calls = [mock.call(get_config().items()), mock.call(spark_settings)]
        created_config.setAll.assert_has_calls(calls, any_order=False)
        self.assertEqual(created_config.setAll.call_count, 2)
        self.assertIs(eskapade_config, created_config, 'incorrect config set')
        mock_sm.reset_mock()
        mock_io_path.reset_mock()
        created_config.reset_mock()

    def test_get_spark_streaming_context(self):
        """Test getting Spark Streaming Context"""

        # create mock configuration
        mock_sm = mock.Mock(name='spark_manager')
        mock_stream = mock.Mock(name='spark_streaming_context')
        current_stream = mock.Mock(name='retrieved_streaming_context')

        # test returning configuration of non-existing Spark streaming context
        mock_sm._stream = None
        current_stream = SparkManager.spark_streaming_context.__get__(mock_sm)
        self.assertIs(current_stream, None)

        # test returning configuration of existing Spark streaming context
        mock_sm._stream = mock_stream
        current_stream = SparkManager.spark_streaming_context.__get__(mock_sm)
        self.assertIs(current_stream, mock_stream)

    def test_set_spark_streaming_context(self):
        """Test setting Spark Streaming Context"""

        # create mock configuration
        mock_sm = mock.Mock(name='spark_manager')
        mock_stream = mock.Mock(name='existing_streaming_context')
        new_stream = mock.Mock(name='new_streaming_context', spec=pyspark.streaming.StreamingContext)

        # test setting new Spark Streaming Context
        mock_sm._stream = None
        SparkManager.spark_streaming_context.__set__(mock_sm, new_stream)
        self.assertIs(mock_sm._stream, new_stream)

        # test setting new Spark Streaming Context with existing context
        mock_sm._stream = mock_stream
        with self.assertRaises(RuntimeError):
            SparkManager.spark_streaming_context.__set__(mock_sm, new_stream)

        # test setting custom configuration of incorrect type
        mock_sm._stream = None
        wrong_stream = mock.Mock(name='wrong_streaming_context', spec=None)
        with self.assertRaises(TypeError):
            SparkManager.spark_streaming_context.__set__(mock_sm, wrong_stream)

    def test_finish(self):
        """Test finishing Spark operations"""

        # create mock Spark manager
        mock_sm = mock.Mock(name='spark_manager')
        mock_session = mock.Mock(name='spark_session')
        mock_sm._session = mock_session

        # test finish
        SparkManager.finish(mock_sm)
        mock_session.stop.assert_called_once_with()
        self.assertIs(mock_sm._session, None)
