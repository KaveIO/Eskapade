import os

from eskapade import ConfigObject
from eskapade import DataStore
from eskapade import process_manager, resources
from eskapade_python.bases import TutorialMacrosTest


class AnalysisTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on analysis tutorial macros"""

    def test_esk201(self):
        self.eskapade_run(resources.tutorial('esk201_readdata.py'))

        ds = process_manager.service(DataStore)

        self.assertTrue('test1' in ds)
        self.assertTrue('test2' in ds)
        self.assertEqual(12, ds['n_test1'])
        self.assertEqual(36, ds['n_test2'])

    def test_esk202(self):
        self.eskapade_run(resources.tutorial('esk202_writedata.py'))

        settings = process_manager.service(ConfigObject)

        ds = process_manager.service(DataStore)

        self.assertEqual(36, ds['n_test'])
        path = settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/tmp3.csv'
        self.assertTrue(os.path.exists(path))
        # check file is non-empty
        statinfo = os.stat(path)
        self.assertTrue(statinfo.st_size > 0)

    def test_esk203(self):
        self.eskapade_run(resources.tutorial('esk203_apply_func_to_pandas_df.py'))

        ds = process_manager.service(DataStore)

        self.assertTrue('transformed_data' in ds)
        df = ds['transformed_data']
        self.assertTrue('xx' in df.columns)
        self.assertTrue('yy' in df.columns)

    def test_esk204(self):
        self.eskapade_run(resources.tutorial('esk204_apply_query_to_pandas_df.py'))

        ds = process_manager.service(DataStore)

        self.assertTrue('outgoing_records' in ds)
        self.assertTrue(ds['n_outgoing_records'] > 0)
        df = ds['outgoing_records']
        self.assertTrue('a' in df.columns)
        self.assertFalse('b' in df.columns)
        self.assertTrue('c' in df.columns)

    def test_esk205(self):
        self.eskapade_run(resources.tutorial('esk205_concatenate_pandas_dfs.py'))

        ds = process_manager.service(DataStore)

        self.assertTrue('outgoing' in ds)
        self.assertEqual(ds['n_outgoing'], 12)

    def test_esk206(self):
        self.eskapade_run(resources.tutorial('esk206_merge_pandas_dfs.py'))

        ds = process_manager.service(DataStore)

        self.assertTrue('outgoing' in ds)
        df = ds['outgoing']
        self.assertEqual(len(df.index), 4)
        self.assertEqual(len(df.columns), 5)

    def test_esk207(self):
        self.eskapade_run(resources.tutorial('esk207_record_vectorizer.py'))

        ds = process_manager.service(DataStore)

        columns = sorted(['x_1', 'x_3', 'x_5', 'x_4', 'y_9', 'y_8', 'y_7', 'y_6', 'y_5', 'y_4'])

        self.assertTrue('vect_test' in ds)
        df = ds['vect_test']
        self.assertEqual(len(df.index), 12)
        self.assertListEqual(sorted(df.columns.tolist()), columns)

    def test_esk208(self):
        self.eskapade_run(resources.tutorial('esk208_record_factorizer.py'))

        ds = process_manager.service(DataStore)

        self.assertTrue('test1' in ds)
        self.assertTrue('test1_fact' in ds)
        self.assertTrue('test1_refact' in ds)
        self.assertTrue('to_original' in ds)
        df1 = ds['test1']
        df2 = ds['test1_refact']
        self.assertEqual(len(df1.index), 12)
        self.assertEqual(len(df2.index), 12)
        self.assertTrue('dummy' in df1.columns)
        self.assertTrue('loc' in df1.columns)
        self.assertTrue('dummy' in df2.columns)
        self.assertTrue('loc' in df2.columns)
        self.assertListEqual(df1['dummy'].values.tolist(), df2['dummy'].values.tolist())
        self.assertListEqual(df1['loc'].values.tolist(), df2['loc'].values.tolist())

    def test_esk209(self):
        self.eskapade_run(resources.tutorial('esk209_read_big_data_itr.py'))

        ds = process_manager.service(DataStore)

        self.assertTrue('test2' in ds)
        self.assertEqual(12, ds['n_test1'])
        self.assertEqual(2, ds['n_test2'])
        self.assertEqual(36, ds['n_sum_test1'])
        self.assertEqual(36, ds['n_sum_test2'])
        self.assertEqual(24, ds['n_merged'])

    def test_esk210(self):
        self.eskapade_run(resources.tutorial('esk210_dataframe_restoration.py'))
        ds = process_manager.service(DataStore)

        # Numpy RW
        self.assertTrue('reloaded_typed_data_np' in ds)
        self.asserTrue((ds['reloaded_typed_data_np'] == ds['typed_data']).all())
        self.asserTrue((ds['reloaded_typed_data_np'].dtypes.values == ds['typed_data'].dtypes.values).all())

        # Feather RW
        self.assertTrue('reloaded_typed_data_ft' in ds)
        self.asserTrue((ds['reloaded_typed_data_ft'] == ds['typed_data']).all())
        self.asserTrue((ds['reloaded_typed_data_ft'].dtypes.values == ds['typed_data'].dtypes.values).all())
