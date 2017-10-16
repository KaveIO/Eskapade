import glob
import os
import random
import re
import shutil
import string
import subprocess
import sys
import unittest

import pandas as pd
import pyspark
from pyspark.sql.types import StructField, LongType, DoubleType, StringType

from eskapade import process_manager, resources, utils, ConfigObject, DataStore
from eskapade.core import persistence
from eskapade.logger import Logger
from eskapade.spark_analysis import SparkManager
from eskapade_python.bases import TutorialMacrosTest


class SparkAnalysisTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on spark-analysis tutorial macros"""

    logger = Logger()

    def setUp(self):
        """Set up test"""

        TutorialMacrosTest.setUp(self)
        settings = process_manager.service(ConfigObject)
        settings['analysisName'] = 'SparkAnalysisTutorialMacrosTest'

        # ensure local testing
        spark_settings = [('spark.app.name', settings['analysisName']),
                          ('spark.master', 'local[*]'),
                          ('spark.driver.host', 'localhost')]
        process_manager.service(SparkManager).create_session(eskapade_settings=settings, spark_settings=spark_settings)

    def tearDown(self):
        """Tear down test environment"""

        process_manager.service(SparkManager).finish()

    def test_esk601(self):
        """Test Esk-601: Configure Spark"""

        # ensure no running Spark instance
        process_manager.service(SparkManager).finish()

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk601_spark_configuration.py'))

        sc = process_manager.service(SparkManager).get_session().sparkContext

        # check configuration
        self.assertEqual(sc.getConf().get('spark.app.name'), 'esk601_spark_configuration_link',
                         'SparkConf.setAll() not picked up correctly')
        self.assertEqual(sc.getConf().get('spark.master'), 'local[42]',
                         'SparkConf.setAll() not picked up correctly')
        self.assertEqual(sc.getConf().get('spark.driver.host'), '127.0.0.1',
                         'SparkConf.setAll() not picked up correctly')

        # stop spark manager
        process_manager.service(SparkManager).finish()

    def test_esk602(self):
        """Test Esk-602: Read CSV files into a Spark data frame"""

        # check if running in local mode
        sc = process_manager.service(SparkManager).get_session().sparkContext
        self.assertRegex(
            sc.getConf().get('spark.master', ''),
            'local\[[.*]\]', 'Spark not running in local mode, required for testing with local files')

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk602_read_csv_to_spark_df.py'))
        ds = process_manager.service(DataStore)

        # check data frame
        self.assertIn('spark_df', ds, 'no object with key "spark_df" in data store')
        self.assertIsInstance(ds['spark_df'], pyspark.sql.DataFrame, '"spark_df" is not a Spark data frame')
        self.assertEqual(ds['spark_df'].rdd.getNumPartitions(), 5, 'unexpected number of partitions in data frame')
        self.assertEqual(ds['spark_df'].count(), 12, 'unexpected number of rows in data frame')
        self.assertListEqual(ds['spark_df'].columns, ['date', 'loc', 'x', 'y'], 'unexpected columns in data frame')
        self.assertSetEqual(set((r['date'], r['loc']) for r in ds['spark_df'].collect()),
                            set([(20090101, 'a'), (20090102, 'b'), (20090103, 'c'), (20090104, 'd'), (20090104, 'e'),
                                 (20090106, 'a'), (20090107, 'b'), (20090107, 'c'), (20090107, 'd'), (20090108, 'e'),
                                 (20090109, 'e'), (20090109, 'f')]),
                            'unexpected values in date/loc columns')

    def test_esk603(self):
        """Test Esk-603: Write Spark data to CSV"""

        # check if running in local mode
        sc = process_manager.service(SparkManager).get_session().sparkContext
        self.assertRegex(
            sc.getConf().get('spark.master', ''),
            'local\[[.*]\]', 'Spark not running in local mode, required for testing with local files')

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk603_write_spark_data_to_csv.py'))

        # read output data
        results_data_path = persistence.io_dir('results_data', process_manager.service(ConfigObject).io_conf())
        names = []
        headers = []
        contents = []
        csv_dirs = glob.glob('{}/*'.format(results_data_path))
        self.assertEqual(len(csv_dirs), 3, 'expected to find three CSV output directories')
        for csv_dir in csv_dirs:
            names.append(os.path.basename(csv_dir))
            csv_files = glob.glob('{}/part*'.format(csv_dir))
            self.assertEqual(len(csv_files), 1, 'expected to find only one CSV file in "{}"'.format(names[-1]))
            with open(csv_files[0]) as csv:
                contents.append([l.strip().split(',') for l in csv])
                self.assertEqual(len(contents[-1]), 101, 'unexpected number of lines in "{}" CSV'.format(names[-1]))
                headers.append(contents[-1][0])
                contents[-1] = sorted(contents[-1][1:])

        # check output data
        self.assertListEqual(headers[0], ['index', 'foo', 'bar'], 'unexpected CSV header for "{}"'.format(names[0]))
        self.assertListEqual(contents[0],
                             sorted([str(it), 'foo{:d}'.format(it), str((it + 1) / 2.)] for it in range(100)),
                             'unexpected CSV content for "{}"'.format(names[0]))
        for name, head, cont in zip(names[1:], headers[1:], contents[1:]):
            self.assertListEqual(head, headers[0],
                                 'CSV header of "{0:s}" differs from header of "{1:s}"'.format(name, names[0]))
            self.assertListEqual(cont, contents[0],
                                 'CSV content of "{0:s}" differs from content of "{1:s}"'.format(name, names[0]))

    @unittest.skip('The new chain interface does not have a method get. '
                   'BTW how do I know which chains/links are defined?')
    def test_esk604(self):
        """Test Esk-604: Execute Spark-SQL query"""

        # check if running in local mode
        sc = process_manager.service(SparkManager).get_session().sparkContext
        self.assertRegex(
            sc.getConf().get('spark.master', ''),
            'local\[[.*]\]', 'Spark not running in local mode, required for testing with local files')

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk604_spark_execute_query.py'))
        ds = process_manager.service(DataStore)

        # check data frame
        self.assertIn('spark_df_sql', ds, 'no object with key "spark_df_sql" in data store')
        self.assertIsInstance(ds['spark_df_sql'], pyspark.sql.DataFrame, '"spark_df_sql" is not a Spark data frame')
        self.assertEqual(ds['spark_df_sql'].count(), 4, 'unexpected number of rows in filtered data frame')
        self.assertListEqual(ds['spark_df_sql'].columns, ['loc', 'sumx', 'sumy'], 'unexpected columns in data frame')
        self.assertEqual(ds['spark_df_sql'].schema, process_manager.get_chain('ApplySQL').get_link('SparkSQL').schema,
                         'schema of data frame does not correspond to schema stored in link')
        self.assertSetEqual(set(tuple(r) for r in ds['spark_df_sql'].collect()),
                            set([('e', 10, 15), ('d', 2, 11), ('b', 6, 16), ('a', 2, 18)]),
                            'unexpected values in loc/sumx/sumy columns')

    def test_esk605(self):
        """Test Esk-605: Create Spark data frame"""

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk605_create_spark_df.py'))
        ds = process_manager.service(DataStore)

        # check created data frames
        cols = (StructField('index', LongType()), StructField('foo', StringType()), StructField('bar', DoubleType()))
        rows = [(it, 'foo{:d}'.format(it), (it + 1) / 2.) for it in range(20, 100)]
        for key in ('rows_df', 'rdd_df', 'df_df', 'pd_df'):
            self.assertIn(key, ds, 'no object with key {} in data store'.format(key))
            df = ds[key]
            self.assertIsInstance(df, pyspark.sql.DataFrame,
                                  'object with key {0:s} is not a data frame (type {1!s})'.format(key, type(df)))
            self.assertTupleEqual(tuple(df.schema), cols, 'unexpected data-frame schema for {}'.format(key))
            self.assertListEqual(sorted(tuple(r) for r in df.collect()), rows,
                                 'unexpected data-frame content for {}'.format(key))
            self.assertTrue(df.is_cached, 'data frame {} is not cached'.format(key))
            self.assertLessEqual(df.rdd.getNumPartitions(), 2,
                                 'unexpected number of data-frame partitions for {}'.format(key))

    def test_esk606(self):
        """Test Esk-606: Convert Spark data frame"""

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk606_convert_spark_df.py'))
        ds = process_manager.service(DataStore)

        # define types of stored data sets
        data_types = {'df': pyspark.sql.DataFrame, 'rdd': pyspark.RDD, 'list': list, 'pd': pd.DataFrame}

        # define functions to obtain data-frame content
        content_funcs = {'df': lambda d: sorted(d.rdd.map(tuple).collect()),
                         'rdd': lambda d: sorted(d.collect()),
                         'list': lambda d: sorted(d),
                         'pd': lambda d: sorted(map(tuple, d.values))}

        # check input data
        self.assertIn('df', ds, 'no data found with key "df"')
        self.assertIsInstance(ds['df'], pyspark.sql.DataFrame, 'unexpected type for input data frame')

        # check created data sets
        rows = [(it, 'foo{:d}'.format(it), (it + 1) / 2.) for it in range(20, 100)]
        for key, dtype in data_types.items():
            # check content
            dkey = '{}_output'.format(key)
            self.assertIn(dkey, ds, 'no data found with key "{}"'.format(dkey))
            self.assertIsInstance(ds[dkey], dtype, 'unexpected type for "{}" data'.format(key))
            self.assertListEqual(content_funcs[key](ds[dkey]), rows, 'unexpected content for "{}" data'.format(key))

            # check schema
            skey = '{}_schema'.format(key)
            self.assertIn(skey, ds, 'no schema found with key {}'.format(skey))
            self.assertListEqual(list(ds[skey]), list(ds['df'].schema), 'unexpected schema for "{}" data'.format(key))

    def test_esk607(self):
        """Test Esk-607: Add column to Spark dataframe"""

        # check if running in local mode
        sc = process_manager.service(SparkManager).get_session().sparkContext
        self.assertRegex(
            sc.getConf().get('spark.master', ''),
            'local\[[.*]\]', 'Spark not running in local mode, required for testing with local files')

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk607_spark_with_column.py'))
        ds = process_manager.service(DataStore)

        # check data frame
        self.assertIn('new_spark_df', ds, 'no object with key "new_spark_df" in data store')
        self.assertIsInstance(ds['new_spark_df'], pyspark.sql.DataFrame, '"new_spark_df" is not a Spark data frame')
        self.assertEqual(ds['new_spark_df'].count(), 5, 'unexpected number of rows in filtered data frame')
        self.assertListEqual(ds['new_spark_df'].columns, ['dummy', 'date', 'loc', 'x', 'y', 'pow_xy1', 'pow_xy2'],
                             'unexpected columns in data frame')
        self.assertSetEqual(set(tuple(r) for r in ds['new_spark_df'].collect()),
                            set([('bla', 20090103, 'c', 5, 7, 78125.0, 78125.0),
                                 ('bal', 20090102, 'b', 3, 8, 6561.0, 6561.0),
                                 ('flo', 20090104, 'e', 3, 5, 243.0, 243.0),
                                 ('bar', 20090101, 'a', 1, 9, 1.0, 1.0),
                                 ('foo', 20090104, 'd', 1, 6, 1.0, 1.0)]),
                            'unexpected values in columns')

    # FIXME: Test fails because of the bugs in histogrammar package. Apply the patches before running the test.
    @unittest.skip('Test fails because of the bugs in histogrammar package. Apply the patches before running the test')
    def test_esk608(self):
        """Test Esk-608: Execute Spark histogram filling macro"""

        # check if required Python and Java libraries are made available to worker nodes
        sc = process_manager.service(SparkManager).get_session().sparkContext
        self.assertRegex(
            sc.getConf().get('spark.master', ''),
            'local\[[.*]\]', 'Spark not running in local mode, required for testing with local files')
        self.assertRegex(
            sc.getConf().get('spark.jars.packages', ''),
            'org.diana-hep:histogrammar-sparksql_2.11:1.0.4',
            'org.diana-hep:histogrammar-sparksql_2.11:1.0.4 missing from spark.jars.packages, test_esk608 will fail')

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk608_spark_histogrammar.py'))
        ds = process_manager.service(DataStore)
        settings = process_manager.service(ConfigObject)

        # check data frame
        self.assertIn('spark_df', ds, 'no object with key "spark_df" in data store')
        self.assertIsInstance(ds['spark_df'], pyspark.sql.DataFrame, '"spark_df" is not a Spark data frame')
        self.assertEqual(ds['spark_df'].count(), 12, 'unexpected number of rows in data frame')
        self.assertListEqual(sorted(ds['spark_df'].columns), sorted(['date', 'loc', 'x', 'y']),
                             'unexpected columns in data frame')

        # data-generation checks
        self.assertIn('hist', ds)
        self.assertIsInstance(ds['hist'], dict)
        col_names = ['date', 'x', 'y', 'loc', 'x:y']
        self.assertListEqual(sorted(ds['hist'].keys()), sorted(col_names))

        # data-summary checks
        f_bases = ['date', 'x', 'y', 'loc', 'x_vs_y']
        file_names = ['report.tex'] + ['hist_{}.pdf'.format(col) for col in f_bases]
        for fname in file_names:
            path = persistence.io_path('results_data', settings.io_conf(), 'report/{}'.format(fname))
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk609(self):
        """Test Esk-609: Map data-frame groups"""

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk609_map_df_groups.py'))
        ds = process_manager.service(DataStore)

        # check input data
        for key in ('map_rdd', 'flat_map_rdd'):
            self.assertIn(key, ds, 'no data found with key "{}"'.format(key))
            self.assertIsInstance(ds[key], pyspark.RDD,
                                  'object "{0:s}" is not an RDD (type "{1!s}")'.format(key, type(ds[key])))

        # sums of "bar" variable
        bar_sums = [(0, 27.5), (1, 77.5), (2, 127.5), (3, 177.5), (4, 227.5), (5, 277.5), (6, 327.5), (7, 377.5),
                    (8, 427.5), (9, 477.5)]
        flmap_rows = [(it, 'foo{:d}'.format(it), (it + 1) / 2., bar_sums[it // 10][1]) for it in range(100)]

        # check mapped data frames
        self.assertListEqual(sorted(ds['map_rdd'].collect()), bar_sums, 'unexpected values in "map_rdd"')
        self.assertListEqual(sorted(ds['flat_map_rdd'].collect()), flmap_rows, 'unexpected values in "flat_map_rdd"')

    def test_esk610(self):
        """Test Esk-610: Spark Streaming word count"""

        # this test relies on linux shell scripts to create file stream
        if (sys.platform != 'linux') and (sys.platform != 'darwin'):
            self.logger.debug('skipping test_esk610 for non-unix {} platform'.format(sys.platform))
            return

        # check if running in local mode
        sc = process_manager.service(SparkManager).get_session().sparkContext
        self.assertRegex(
            sc.getConf().get('spark.master', ''),
            'local\[[.*]\]', 'Spark not running in local mode, required for testing with local files')

        # create test dir
        tmpdir = '/tmp/eskapade_stream_test'
        os.mkdir(tmpdir)

        # create a file stream
        tmpfile = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
        cmd = 'for i in $(seq -f \"%05g\" 0 1000); \
                do echo \'Hello world\' > "{}"/"{}"_$i.dummy; \
                        sleep 1; done'.format(tmpdir, tmpfile)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # run eskapade
        self.eskapade_run(resources.tutorial('esk610_spark_streaming_wordcount.py'))
        ds = process_manager.service(DataStore)

        # end file stream
        p.kill()

        # check if file stream was properly executed
        stdout, stderr = p.communicate()
        self.assertEqual(stdout, b'', 'unexpected stdout output {}'.format(stdout))
        self.assertEqual(stderr, b'', 'unexpected stderr output {}'.format(stderr))

        # check if stream was setup correctly (that's all we can do - the data itself is gone)
        self.assertIsInstance(ds['dstream'], pyspark.streaming.DStream)

        # read and check output data
        results_data_path = persistence.io_dir('results_data', process_manager.service(ConfigObject).io_conf())
        names = []
        contents = []
        csv_dirs = glob.glob('{}/dstream/wordcount-*.txt'.format(results_data_path))
        self.assertGreater(len(csv_dirs), 0, 'expected to find CSV output directories')
        for csv_dir in csv_dirs:
            names.append(os.path.basename(csv_dir))
            csv_files = glob.glob('{}/part*'.format(csv_dir))
            # self.assertEqual(len(csv_files), 1, 'expected to find exactly one CSV file in "{}"'.format(names[-1]))
            if csv_files:
                with open(csv_files[0]) as csv:
                    record = [l for l in csv]
                    if record:  # empty records are allowed (because of timing differences)
                        self.assertRegex(record[0], 'Hello', 'Expected \'Hello\' as in \'Hello world\'')
                        self.assertRegex(record[1], 'world', 'Expected \'world\' as in \'Hello world\'')
                    contents.append(record[:])
        self.assertGreater(len(contents), 0, 'expected ~ten items (each second a streaming RDD) - depending on timing')

        # clean up files
        shutil.rmtree(tmpdir)
