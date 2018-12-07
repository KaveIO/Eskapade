import numpy as np
import pandas as pd
import unittest
import unittest.mock as mock

from eskapade import data_mimic


class TestKdePreperation(unittest.TestCase):

    def setUp(self):
        from eskapade import process_manager, DataStore

        self.ds = process_manager.service(DataStore)

        self.ds['new_column_order'] = ['a', 'b', 'c', 'd']
        self.ds['df'] = pd.DataFrame(np.hstack((np.random.normal(0, 1, (100, 1)),
                                     np.random.normal(0, 1, (100, 1)),
                                     np.random.normal(0, 1, (100, 1)),
                                     np.random.randint(0, 4, (100, 1)))), columns=self.ds['new_column_order'])

    def tearDown(self):
        from escore.core import execution
        execution.reset_eskapade()

    @mock.patch('eskapade.data_mimic.data_mimic_util.transform_to_normal')
    @mock.patch('eskapade.data_mimic.data_mimic_util.append_extremes')
    @mock.patch('eskapade.data_mimic.data_mimic_util.remove_nans')
    @mock.patch('eskapade.data_mimic.data_mimic_util.smooth_peaks')
    @mock.patch('eskapade.data_mimic.data_mimic_util.find_peaks')
    def test_evaluater(self, mock_find_peaks, mock_smooth_peaks, mock_remove_nans,
                       mock_append_extremes, mock_tr_normal):

        mock_find_peaks.return_value = 1
        mock_smooth_peaks.return_value = self.ds['df'].values
        mock_remove_nans.return_value = self.ds['df'].values
        mock_append_extremes.return_value = (self.ds['df'].values, 1, 3)
        mock_tr_normal.return_value = self.ds['df'].values, 4

        kdeprep = data_mimic.KDEPreparation(read_key='df',
                                            data_store_key='data',
                                            data_smoothed_store_key='data_smoothed',
                                            data_no_nans_store_key='data_no_nans',
                                            data_normalized_store_key='data_normalized',
                                            maps_store_key='maps',
                                            qts_store_key='qts',
                                            new_column_order_store_key='new_column_order',
                                            ids_store_key='ids',
                                            unordered_categorical_columns=['d'],
                                            ordered_categorical_columns=[],
                                            continuous_columns=['a', 'b', 'c'],
                                            string_columns=[],
                                            count=1,
                                            extremes_fraction=0.15,
                                            smoothing_fraction=0.0002)
        kdeprep.initialize()
        kdeprep.execute()

        mock_find_peaks.assert_called()
        mock_smooth_peaks.assert_called()
        mock_remove_nans.assert_called()
        mock_append_extremes.assert_called()
        mock_tr_normal.assert_called()

        # assert ds is filled

        self.assertIsNotNone(self.ds['data_smoothed']), "data_smoothed not saved in data store"
        self.assertIsNotNone(self.ds['data_no_nans']), "data_no_nans not saved in data store"
        self.assertIsNotNone(self.ds['data_normalized']), "data_normalized not saved in data store"
        self.assertIsNotNone(self.ds['ids']), "ids not saved in data store"
        self.assertIsNotNone(self.ds['qts']), "qts not saved in data store"
        self.assertIsNotNone(self.ds['data']), "data not saved in data store"
