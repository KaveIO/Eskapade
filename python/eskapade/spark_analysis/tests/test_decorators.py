import unittest
import unittest.mock as mock

import pyspark

from eskapade.spark_analysis.decorators import CUSTOM_REDUCE_CLASSES, spark_cls_reduce


class DecoratorsTest(unittest.TestCase):
    """Tests for the Spark decorators"""

    def test_reduce(self):
        """Test Spark reduce function"""

        mock_obj = mock.Mock(name='mock_reduce_object')
        red_repr = spark_cls_reduce(mock_obj)
        self.assertEqual(red_repr[0](*red_repr[1]), str(mock_obj), 'unexpected reduced representation')

    def test_reduce_classes(self):
        """Test reduce function of Spark classes"""

        for red_cls in CUSTOM_REDUCE_CLASSES:
            red_meth = getattr(red_cls, '__reduce__', None)
            self.assertIs(red_meth, spark_cls_reduce, 'unexpected reduce method for {}'.format(red_cls.__name__))

    def test_df_dec(self):
        """Test Spark data-frame decorators"""

        # create a mock column and a mock data frame
        mock_col = mock.Mock(name='column')
        mock_df = mock.Mock(name='data_frame')
        mock_col.__str__ = pyspark.sql.Column.__str__
        mock_col.__format__ = pyspark.sql.Column.__format__
        mock_df.__contains__ = pyspark.sql.DataFrame.__contains__
        mock_col._jc.toString.return_value = 'column_name'
        mock_df.columns = ('col', 'column_name')

        # test decorators
        self.assertEqual(str(mock_col), 'column_name', 'unexpected string result for column')
        self.assertEqual('|{:>12s}|'.format(mock_col), '| column_name|', 'unexpected format result for column')
        self.assertListEqual([c in mock_df for c in ('col', mock_col, 'foo')], [True, True, False],
                             'unexpected behaviour of data-frame "in" operator')
