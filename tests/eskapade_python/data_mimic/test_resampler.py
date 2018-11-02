import numpy as np
import unittest
import unittest.mock as mock

from eskapade import data_mimic


class TestResampler(unittest.TestCase):

    def setUp(self):
        from eskapade import process_manager, DataStore

        self.ds = process_manager.service(DataStore)

        self.ds['new_column_order'] = ['a', 'b', 'c', 'd']
        self.ds['unordered_categorical_i'] = [3]
        self.ds['ordered_categorical_i'] = [2]
        self.ds['continuous_i'] = [0, 1]
        self.ds['data'] = np.hstack((np.random.normal(0, 1, (100, 1)),
                                     np.random.normal(0, 1, (100, 1)),
                                     np.random.randint(0, 4, (100, 1)),
                                     np.random.randint(0, 4, (100, 1))))
        self.ds['data_normalized'] = self.ds['data'].copy()
        self.ds['data_normalized_pca'] = self.ds['data_normalized'].copy()
        self.ds['maps'] = {}
        self.ds['qts'] = 1
        self.ds['bw'] = np.random.randint(0, 1, 4).tolist()
        self.ds['ids'] = np.random.randint(0, 100, 10)

    def tearDown(self):
        from eskapade.core import execution
        execution.reset_eskapade()

    @mock.patch('eskapade.data_mimic.data_mimic_util.scale_and_invert_normal_transformation')
    @mock.patch('eskapade.data_mimic.data_mimic_util.kde_resample')
    @mock.patch('eskapade.data_mimic.data_mimic_util.insert_back_nans')
    def test_kde_all_types(self, mock_insert_back_nans, mock_resample, mock_scale_and_invert):

        mock_insert_back_nans.resturn_value = self.ds['data']
        # mock_resample.resturn_value = (self.ds['data'], np.random.randint(0, 100, 100))
        mock_resample.return_value = (self.ds['data'].copy, np.random.randint(0, 100, 100))
        mock_scale_and_invert.resturn_value = np.random.normal(0, 1, 100)

        resampler = data_mimic.Resampler(data_normalized_read_key='data_normalized',
                                         data_normalized_pca_read_key='data_normalized_pca',
                                         data_read_key='data',
                                         bws_read_key='bw',
                                         qts_read_key='qts',
                                         new_column_order_read_key='new_column_order',
                                         maps_read_key='maps',
                                         ids_read_key='ids',
                                         pca_read_key='pca_model',
                                         do_pca=False,
                                         df_resample_store_key='df_resample',
                                         resample_store_key='data_resample')
        resampler.initialize()
        resampler.execute()

        mock_resample.assert_called_once()
        mock_insert_back_nans.assert_called_once()
        mock_scale_and_invert.assert_called_once()

        self.assertIsNotNone(self.ds['data_resample'])
        self.assertIsNotNone(self.ds['df_resample'])
