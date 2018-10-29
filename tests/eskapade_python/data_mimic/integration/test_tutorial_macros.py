import numpy as np
import pandas as pd

from eskapade import process_manager, resources, DataStore
from eskapade_python.bases import TutorialMacrosTest


class DataMimicTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on data quality tutorial macros"""

    def test_esk701(self):

        # run Eskapade

        self.eskapade_run(resources.tutorial('esk701_mimic_data.py'))
        ds = process_manager.service(DataStore)

        # -- make sure all was saved to the data store
        self.assertIn('df', ds)
        self.assertIn('ids', ds)
        self.assertIn('maps', ds)
        self.assertIn('new_column_order', ds)
        self.assertIn('qts', ds)
        self.assertIn('data', ds)
        self.assertIn('data_smoothed', ds)
        self.assertIn('data_no_nans', ds)
        self.assertIn('data_normalized', ds)
        self.assertIn('unordered_categorical_i', ds)
        self.assertIn('ordered_categorical_i', ds)
        self.assertIn('continuous_i', ds)
        self.assertIn('bw', ds)
        self.assertIn('data_resample', ds)
        self.assertIn('df_resample', ds)
        self.assertIn('chi2', ds)
        self.assertIn('p_value', ds)

        # -- make sure they're of the right type
        self.assertIsInstance(ds['df'], pd.DataFrame)
        self.assertIsInstance(ds['ids'], np.ndarray)
        self.assertIsInstance(ds['maps'], dict)
        self.assertIsInstance(ds['new_column_order'], list)
        self.assertIsInstance(ds['qts'], list)
        self.assertIsInstance(ds['data'], np.ndarray)
        self.assertIsInstance(ds['data_smoothed'], np.ndarray)
        self.assertIsInstance(ds['data_no_nans'], np.ndarray)
        self.assertIsInstance(ds['data_normalized'], np.ndarray)
        self.assertIsInstance(ds['unordered_categorical_i'], list)
        self.assertIsInstance(ds['ordered_categorical_i'], list)
        self.assertIsInstance(ds['continuous_i'], list)
        self.assertIsInstance(ds['bw'], np.ndarray)
        self.assertIsInstance(ds['data_resample'], np.ndarray)
        self.assertIsInstance(ds['df_resample'], pd.DataFrame)
        self.assertIsInstance(ds['chi2'], np.float64)
        self.assertIsInstance(ds['p_value'], np.float64)

        self.assertEqual(ds['df'].shape[1], 7)
        self.assertEqual(ds['data'].shape[1], 7)
        self.assertEqual(ds['data_smoothed'].shape[1], 7)
        self.assertEqual(ds['data_no_nans'].shape[1], 7)
        self.assertEqual(ds['data_normalized'].shape[1], 3)
        self.assertEqual(len(ds['unordered_categorical_i']), 2)
        self.assertEqual(len(ds['ordered_categorical_i']), 2)
        self.assertEqual(len(ds['continuous_i']), 3)
        self.assertEqual(ds['bw'].shape[0], 7)
        self.assertEqual(ds['data_resample'].shape[1], 7)
        self.assertEqual(ds['df_resample'].shape[1], 8)

        # check if the generated data indeed contains and strings
        self.assertTrue((ds['df'].isnull().sum() > 0).any())
        self.assertEqual([pd.api.types.infer_dtype(ds['df'][x]) for x in ds['df'].columns],
                         ['floating', 'floating', 'floating',
                          'string', 'string', 'floating', 'integer'])
        self.assertEqual([pd.api.types.infer_dtype(ds['df_resample'][x]) for x in ds['df_resample'].columns],
                         ['string', 'string', 'floating', 'floating',
                          'floating', 'floating', 'floating', 'integer'])

        self.assertEqual([len(np.unique(ds['df'].dropna()[x])) for x in ds['df'].columns[-4:]],
                         [4, 2, 3, 5])
        self.assertEqual([len(np.unique(ds['df_resample'].dropna()[x])) for x in ds['df_resample'].columns[:4]],
                         [4, 2, 3, 5])

        self.assertTrue(np.array([x in ds['new_column_order'] for x in ds['df'].columns]).all())
        self.assertTrue(np.array([x in ds['new_column_order'] for x in ds['df_resample'].columns[:-1]]).all())

    def test_esk703_only_ordered(self):

        # run Eskapade

        self.eskapade_run(resources.tutorial('esk703_mimic_data_only_unordered.py'))
        ds = process_manager.service(DataStore)

        # -- make sure all was saved to the data store
        self.assertIn('df', ds)
        self.assertIn('ids', ds)
        self.assertIn('maps', ds)
        self.assertIn('new_column_order', ds)
        self.assertIn('qts', ds)
        self.assertIn('data', ds)
        self.assertIn('data_smoothed', ds)
        self.assertIn('data_no_nans', ds)
        self.assertIn('data_normalized', ds)
        self.assertIn('unordered_categorical_i', ds)
        self.assertIn('ordered_categorical_i', ds)
        self.assertIn('continuous_i', ds)
        self.assertIn('bw', ds)
        self.assertIn('data_resample', ds)
        self.assertIn('df_resample', ds)
        self.assertIn('chi2', ds)
        self.assertIn('p_value', ds)

        # -- make sure they're of the right type
        self.assertIsInstance(ds['df'], pd.DataFrame)
        self.assertIsInstance(ds['ids'], np.ndarray)
        self.assertIsInstance(ds['maps'], dict)
        self.assertIsInstance(ds['new_column_order'], list)
        self.assertIsInstance(ds['qts'], list)
        self.assertIsInstance(ds['data'], np.ndarray)
        self.assertIsInstance(ds['data_smoothed'], np.ndarray)
        self.assertIsInstance(ds['data_no_nans'], np.ndarray)
        self.assertIsInstance(ds['data_normalized'], list)
        self.assertIsInstance(ds['unordered_categorical_i'], list)
        self.assertIsInstance(ds['ordered_categorical_i'], list)
        self.assertIsInstance(ds['continuous_i'], list)
        self.assertIsInstance(ds['bw'], np.ndarray)
        self.assertIsInstance(ds['data_resample'], np.ndarray)
        self.assertIsInstance(ds['df_resample'], pd.DataFrame)
        self.assertIsInstance(ds['chi2'], np.float64)
        self.assertIsInstance(ds['p_value'], np.float64)

        self.assertEqual(ds['df'].shape[1], 2)
        self.assertEqual(ds['data'].shape[1], 2)
        self.assertEqual(ds['data_smoothed'].shape[1], 2)
        self.assertEqual(ds['data_no_nans'], 2)
        self.assertEqual(ds['data_normalized'], 0)
        self.assertEqual(len(ds['unordered_categorical_i']), 2)
        self.assertEqual(len(ds['ordered_categorical_i']), 0)
        self.assertEqual(len(ds['continuous_i']), 0)
        self.assertEqual(ds['bw'].shape[0], 7)
        self.assertEqual(ds['data_resample'].shape[1], 7)
        self.assertEqual(ds['df_resample'].shape[1], 8)

        # check if the generated data indeed contains and strings
        self.assertTrue((ds['df'].isnull().sum() > 0).any())
        self.assertEqual([pd.api.types.infer_dtype(ds['df'][x]) for x in ds['df'].columns],
                         ['floating', 'floating', 'floating',
                          'string', 'string', 'floating', 'integer'])
        self.assertEqual([pd.api.types.infer_dtype(ds['df_resample'][x]) for x in ds['df_resample'].columns],
                         ['string', 'string', 'floating', 'floating',
                          'floating', 'floating', 'floating', 'integer'])

        self.assertEqual([len(np.unique(ds['df'].dropna()[x])) for x in ds['df'].columns[-4:]],
                         [4, 2, 3, 5])
        self.assertEqual([len(np.unique(ds['df_resample'].dropna()[x])) for x in ds['df_resample'].columns[:4]],
                         [4, 2, 3, 5])

        self.assertTrue(np.array([x in ds['new_column_order'] for x in ds['df'].columns]).all())
        self.assertTrue(np.array([x in ds['new_column_order'] for x in ds['df_resample'].columns[:-1]]).all())