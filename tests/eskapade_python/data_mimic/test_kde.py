import numpy as np
import unittest
import unittest.mock as mock

from eskapade import data_mimic


class TestKernelDensityEstimation(unittest.TestCase):

    def setUp(self):
        from eskapade import process_manager, DataStore

        self.ds = process_manager.service(DataStore)

        self.ds['new_column_order'] = ['a', 'b', 'c', 'd']
        self.ds['unordered_categorical_i'] = [3]
        self.ds['ordered_categorical_i'] = [2]
        self.ds['continuous_i'] = [0, 1]
        self.ds['data_no_nans'] = np.hstack((np.random.normal(0, 1, (100, 1)),
                                             np.random.normal(0, 1, (100, 1)),
                                             np.random.randint(0, 4, (100, 1)),
                                             np.random.randint(0, 4, (100, 1))))
        self.ds['data_normalized'] = self.ds['data_no_nans'].copy()
        self.ds['data_normalized_pca'] = self.ds['data_normalized'].copy()

    def tearDown(self):
        from eskapade.core import execution
        execution.reset_eskapade()

    @mock.patch('statsmodels.nonparametric.kernel_density.KDEMultivariate')
    @mock.patch('eskapade.data_mimic.data_mimic_util.kde_only_unordered_categorical')
    def test_kde_all_types(self, mock_only_unordered, mock_statsm_kde):

        mock_only_unordered.bw = np.random.normal(0, .2, len(self.ds['unordered_categorical_i']))
        mock_statsm_kde.bw = np.random.normal(0, .2, self.ds['data_no_nans'].shape[1])

        kde = data_mimic.KernelDensityEstimation(data_no_nans_read_key='data_no_nans',
                                                 data_normalized_read_key='data_normalized',
                                                 data_normalized_pca_read_key='data_normalized_pca',
                                                 do_pca=False,
                                                 store_key='bw')
        kde.initialize()
        kde.execute()

        mock_only_unordered.assert_not_called(), "There are not just unordered categorical columns, \
                                                  kde_only_unordered_categorical should not have been called"
        mock_statsm_kde.assert_called_once(), "There are not just unordered categorical columns, \
                                                Normal KDE should have been called once for all variables"

    @mock.patch('statsmodels.nonparametric.kernel_density.KDEMultivariate')
    @mock.patch('eskapade.data_mimic.data_mimic_util.kde_only_unordered_categorical')
    def test_kde_only_unordered_catagorical(self, mock_only_unordered, mock_statsm_kde):

        self.ds['ordered_categorical_i'] = []
        self.ds['continuous_i'] = []

        mock_only_unordered.return_value = np.random.normal(0, .2, len(self.ds['unordered_categorical_i']))
        mock_statsm_kde.return_value = np.random.normal(0, .2, self.ds['data_no_nans'].shape[1])

        kde = data_mimic.KernelDensityEstimation(data_no_nans_read_key='data_no_nans',
                                                 data_normalized_read_key='data_normalized',
                                                 data_normalized_pca_read_key='data_normalized_pca',
                                                 do_pca=False,
                                                 store_key='bw')
        kde.initialize()
        kde.execute()

        mock_only_unordered.assert_called_once(), "There are just unordered categorical columns, \
                                                  kde_only_unordered_categorical should have been called once \
                                                  for all variables"
        mock_statsm_kde.assert_not_called(), "There are just unordered categorical columns, \
                                                Normal KDE should not have been called"
