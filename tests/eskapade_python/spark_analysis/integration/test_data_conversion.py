import datetime
from collections import OrderedDict as odict

import pandas as pd
import pyspark
from pyspark.sql.types import StructField, StructType, LongType, DoubleType, StringType, TimestampType

from eskapade import process_manager, ConfigObject
from eskapade.spark_analysis.data_conversion import create_spark_df, df_schema
from eskapade.spark_analysis.spark_manager import SparkManager
from eskapade_python.bases import IntegrationTest


class DataConversionTest(IntegrationTest):
    """Tests of data-conversion functions"""

    def setUp(self):
        """Setup test environment"""

        settings = process_manager.service(ConfigObject)
        settings['analysisName'] = 'DataConversionTest'
        # ensure local testing
        spark_settings = [('spark.app.name', settings['analysisName']),
                          ('spark.master', 'local[*]'),
                          ('spark.driver.host', 'localhost')]
        process_manager.service(SparkManager).create_session(eskapade_settings=settings, spark_settings=spark_settings)

    def tearDown(self):
        """Tear down test environment"""

        process_manager.service(SparkManager).finish()

    def test_create_spark_df(self):
        """Test creation of a Spark data frame"""

        # create Spark session
        spark = process_manager.service(SparkManager).get_session()

        # create input data
        orig_cols = odict([('_1', LongType()), ('_2', StringType()), ('_3', DoubleType())])
        orig_schema = StructType([StructField(*c) for c in orig_cols.items()])
        data = {'rows': [(it, 'foo{:d}'.format(it), (it + 1) / 2.) for it in range(100)]}
        data['rdd'] = spark.sparkContext.parallelize(data['rows'])  # RDD
        data['df'] = spark.createDataFrame(data['rdd'], schema=list(orig_cols.keys()))  # Spark data frame
        data['pddf'] = pd.DataFrame(data['rows'], columns=list(orig_cols.keys()))  # Pandas data frame

        # define function to set partitions
        def set_num_parts(df, num_parts):
            return df.repartition(num_parts)

        # define process functions
        proc_meths = [('withColumnRenamed', ('_1', 'index'), {}),
                      ('filter', ('index > 19',), {}),
                      (set_num_parts, (), dict(num_parts=2)),
                      ('cache', (), {})]

        # create expected data-frame schema
        schema_fields = tuple(StructField(n, t()) for n, t in (('index', LongType),
                                                               ('_2', StringType),
                                                               ('_3', DoubleType)))

        # test with data types inferred from data
        for key, dset in data.items():
            df = create_spark_df(spark, dset, schema=None, process_methods=proc_meths)
            self.check_created_df('{}-infer_data'.format(key), df, schema_fields, data['rows'])

        # test with data types inferred from row 3
        for key, dset in data.items():
            df = create_spark_df(spark, dset, schema=3, process_methods=proc_meths)
            self.check_created_df('{}-infer_3'.format(key), df, schema_fields, data['rows'])

        # test with column names specified, but types inferred from data
        for key, dset in data.items():
            df = create_spark_df(spark, dset, schema=list(orig_cols.keys()), process_methods=proc_meths)
            self.check_created_df('{}-infer_types'.format(key), df, schema_fields, data['rows'])

        # test with schema dictionary specified
        for key, dset in data.items():
            df = create_spark_df(spark, dset, schema=orig_cols, process_methods=proc_meths)
            self.check_created_df('{}-schema_fields_dict'.format(key), df, schema_fields, data['rows'])

        # test with schema specified
        for key, dset in data.items():
            df = create_spark_df(spark, dset, schema=orig_schema, process_methods=proc_meths)
            self.check_created_df('{}-schema'.format(key), df, schema_fields, data['rows'])

    def check_created_df(self, descr, df, schema_fields, rows):
        """Check a created data frame"""

        self.assertIsInstance(df, pyspark.sql.DataFrame,
                              'object "{0:s}" is not a data frame (type "{1!s}")'.format(descr, type(df)))
        self.assertTupleEqual(tuple(df.schema), schema_fields, 'unexpected data-frame schema for "{}"'.format(descr))
        self.assertListEqual(sorted(tuple(r) for r in df.collect()), rows[20:],
                             'unexpected data-frame content for "{}"'.format(descr))
        self.assertTrue(df.is_cached, 'data frame "{}" is not cached'.format(descr))
        self.assertEqual(df.rdd.getNumPartitions(), 2,
                         'unexpected number of data-frame partitions for "{}"'.format(descr))

    def test_df_schema(self):
        """Test creation of a data-frame schema"""

        # create reference schema
        ref_sub_schema = StructType([StructField(n, t) for n, t in [('long1', LongType()), ('double1', DoubleType())]])
        ref_cols = [('long', LongType()), ('double', DoubleType()), ('string', StringType()),
                    ('timestamp', TimestampType()), ('struct', ref_sub_schema)]
        ref_schema = StructType([StructField(*c) for c in ref_cols])

        # specification with Python types
        specs = {}
        sub_spec = odict([('long1', int), ('double1', float)])
        specs['python'] = odict([('long', int), ('double', float), ('string', str), ('timestamp', datetime.datetime),
                                 ('struct', sub_spec)])

        # specification with Spark types
        sub_spec = odict([('long1', LongType()), ('double1', DoubleType())])
        specs['spark'] = odict([('long', LongType()), ('double', DoubleType()), ('string', StringType()),
                                ('timestamp', TimestampType()), ('struct', sub_spec)])

        # specification with Spark-type classes
        sub_spec = odict([('long1', LongType), ('double1', DoubleType)])
        specs['spark-class'] = odict([('long', LongType), ('double', DoubleType), ('string', StringType),
                                      ('timestamp', TimestampType), ('struct', sub_spec)])

        # specification mixed types
        sub_spec = odict([('long1', LongType), ('double1', float)])
        specs['mixed'] = odict([('long', int), ('double', DoubleType), ('string', str),
                                ('timestamp', TimestampType()), ('struct', sub_spec)])

        # test with different specifications
        for descr, spec in specs.items():
            schema = df_schema(spec)
            self.assertIs(type(schema), type(ref_schema), 'unexpected schema type for "{}"'.format(descr))
            self.assertTupleEqual(tuple(schema), tuple(ref_schema), 'unexpected schema for "{}"'.format(descr))

        # test with incorrect types
        spec = odict([('long', 'long'), ('double', 'double'), ('string', 'string')])
        with self.assertRaises(TypeError):
            df_schema(spec)
