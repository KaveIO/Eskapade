import unittest
import unittest.mock as mock

from eskapade.spark_analysis.functions import (SPARK_UDFS, is_nan, is_inf, to_date_time, to_timestamp, calc_asym,
                                               SPARK_QUERY_FUNCS, spark_sql_func, spark_query_func)

UDFS = dict(is_nan=dict(func=is_nan, ret_type='BooleanType'),
            is_inf=dict(func=is_inf, ret_type='BooleanType'),
            to_date_time=dict(func=to_date_time, ret_type='TimestampType'),
            to_timestamp=dict(func=to_timestamp, ret_type='LongType'),
            calc_asym=dict(func=calc_asym, ret_type='DoubleType'))


class UdfTest(unittest.TestCase):
    """Tests for the Spark user-defined functions"""

    maxDiff = None

    @mock.patch('eskapade.spark_analysis.functions.pd')
    def test_is_nan(self, mock_pd):
        """Test Spark UDF to test for NaN/null/None values"""

        # create Pandas-isnull mock
        mock_pd.isnull.side_effect = lambda x: x in (None, 'pd_isnull')

        # test NaN function
        test_vals = (None, 'pd_isnull', 'None', 'nan', 'foobar', 0)
        test_results = (True,) * 4 + (False,) * 2
        self.assertTupleEqual(tuple(is_nan(x) for x in test_vals), test_results, 'unexpected NaN/null/None results')

    @mock.patch('eskapade.spark_analysis.functions.np')
    def test_is_inf(self, mock_np):
        """Test Spark UDF to test for infinite values"""

        # create NumPy-isinf mock
        def np_isinf(x):
            if not isinstance(x, float):
                raise TypeError('value is not a float')
            return x == float('inf')
        mock_np.isinf.side_effect = np_isinf

        # test infinity function
        test_vals = (float('inf'), 0., 'foo')
        test_results = (True, False, False)
        self.assertTupleEqual(tuple(is_inf(x) for x in test_vals), test_results, 'unexpected infinity results')

    @mock.patch('eskapade.spark_analysis.functions.pd')
    def test_timestamp(self, mock_pd):
        """Test Spark UDF to convert to date/time and timestamp"""

        # create Pandas-timestamp mock
        mock_ts = mock.Mock(name='mock_timestamp')

        def pd_timestamp(dt):
            if dt == '1970-01-01':
                raise ValueError('got 1st January, 1970')
            return mock_ts

        mock_pd.Timestamp.side_effect = pd_timestamp

        # test date/time conversion
        test_vals = (dict(dt=None), dict(dt='1970-01-01'), dict(dt='2017-01-01'),
                     dict(dt='2017-01-01', tz_in='tz_in'), dict(dt='2017-01-01', tz_out='tz_out'),
                     dict(dt='2017-01-01', tz_in='tz_in', tz_out='tz_out'))
        test_results = (None, None, mock_ts, mock_ts.tz_localize(), mock_ts.tz_convert(),
                        mock_ts.tz_localize().tz_convert())
        for vals, res in zip(test_vals, test_results):
            dt = to_date_time(**vals)
            self.assertIs(dt, res.to_datetime() if res else res,
                          'unexpected resulting date/time object for {!s}'.format(vals))
            if 'tz_out' not in vals:
                ts = to_timestamp(**vals)
                self.assertIs(ts, res.value if res else res, 'unexpected resulting timestamp for {!s}'.format(vals))

    def test_calc_asym(self):
        """Test Spark UDF to calculate an asymmetry"""

        # do not mock NumPy float64, but use it for test
        test_vals = ((1.0, 1.0), (1, 1), ('one', 'one'), (0, 0), (1.0, 3.0), (3, 1), (-1, 3), (1, -3), (-1, -3))
        test_results = (0., 0., None, 'nan', 0.5, -0.5, 1.0, -1.0, -0.5)
        for vals, exp_res in zip(test_vals, test_results):
            res = calc_asym(*vals)
            if exp_res is None:
                self.assertIs(res, None, 'asymmetry value for {!s} is not None'.format(vals))
                continue
            self.assertIsInstance(res, float, 'asymmetry value for {!s} is not a float'.format(vals))
            if exp_res == 'nan':
                self.assertNotEqual(res, res, 'asymmetry value for {!s} is not NaN'.format(vals))
                continue
            self.assertAlmostEqual(res, exp_res, places=10,
                                   msg='unexpected asymmetry value for {!s}'.format(vals))

    def test_udf_dict(self):
        """Test dictionary of Spark UDFs"""

        self.assertDictEqual(SPARK_UDFS, UDFS, 'unexpected Spark-UDF dictionary')


class FunctionsTest(unittest.TestCase):
    """Tests for the Spark functions"""

    @mock.patch('pyspark.sql.functions', spec=[])
    def test_spark_sql_func(self, sql_funcs):
        """Test getting Spark-SQL function"""

        # set mock function
        sql_funcs.foo = 'bar'

        # test getting function
        test_vals = (('foo',), ('foo', 'bla'), ('foobar',), ('foobar', 'bla'))
        test_results = ('bar', 'bar', None, 'bla')
        for vals, res in zip(test_vals, test_results):
            if res is not None:
                func = spark_sql_func(*vals)
                self.assertEqual(func, res, 'unexpected function returned for {!s}'.format(vals))
            else:
                with self.assertRaises(RuntimeError):
                    spark_sql_func(*vals)

    @mock.patch.dict('eskapade.spark_analysis.functions.SPARK_QUERY_FUNCS', clear=True)
    def test_spark_query_func(self):
        """Test getting Eskapade Spark-query function"""

        # set mock query function
        SPARK_QUERY_FUNCS['foo'] = 'foo_def_{0:s}_{1:d}'
        SPARK_QUERY_FUNCS['bar'] = ''

        # test getting function
        test_vals = ('foo', 'foo::', '::foo', 'foo::foo_def_alt', 'bar', 'baz::baz_def_{1:d}_{0:s}')
        test_args = ('strarg', 42)
        test_results = ('foo_def_strarg_42', ValueError, ValueError, 'foo_def_strarg_42', RuntimeError,
                        'baz_def_42_strarg')
        for val, res in zip(test_vals, test_results):
            if isinstance(res, str):
                self.assertEqual(spark_query_func(val)(*test_args), res, 'unexpected query result')
            else:
                with self.assertRaises(res):
                    spark_query_func(val)
