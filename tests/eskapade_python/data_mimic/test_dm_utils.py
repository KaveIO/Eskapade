import sys
import numpy as np
import pandas as pd
import unittest
import unittest.mock as mock


from eskapade import data_mimic

# class fake_class(self):

#     def fake_func(self, x):
#         return x

# @mock.patch("")
# class TestFakeClass(unittest.TestCase):

class TestMimicReport(unittest.TestCase):

    def setUp(self):
        from eskapade import process_manager, DataStore
        self.ds = process_manager.service(DataStore)

        # -- make fake data

        self.ds['read_key'] = np.vstack((np.random.normal(0, 1, (100, 1)),
                                         np.random.normal(0, 1, (100, 1)),
                                         np.random.normal(0, 1, (100, 1)),
                                         np.random.randint(0, 4, (100, 1))))
        self.ds['resample_read_key'] = np.vstack((np.random.normal(0, 1, (100, 1)),
                                                  np.random.normal(0, 1, (100, 1)),
                                                  np.random.normal(0, 1, (100, 1)),
                                                  np.random.randint(0, 4, (100, 1))))

        self.ds['new_column_order_read_key'] = ['a', 'b', 'c', 'd']
        self.ds['results_path'] = 1
        self.ds['chi2_read_key'] = 22
        self.ds['p_value_read_key'] = ''
        self.ds['maps_read_key'] = {'d': pd.Series(index=[0, 1, 2, 3], data=['a', 'b', 'c', 'd'])}
        self.ds['key_data_normalized'] = np.random.normal(0, 1, (100, 3))
        self.ds['corr_read_key'] = [pd.DataFrame(np.random.randint(0, 2, (3, 3))),
                                    pd.DataFrame(np.random.randint(0, 2, (3, 3)))]

        self.ds['continuous_i'] = [0, 1, 2]
        self.ds['ordered_categorical_i'] = [3]
        self.ds['unordered_categorical_i'] = []
        self.ds['chis'] = {'a': {'a': {'chi': 0, 'p-value': 1, 'bins': 100}},
                           'b': {'a': {'chi': 0, 'p-value': 1, 'bins': 100}},
                           'c': {'a': {'chi': 0, 'p-value': 1, 'bins': 100}},
                           'd': {'a': {'chi': 0, 'p-value': 1, 'bins': 100}}}
        self.ds['kss'] = {'1': {'a': 1, 'b': 2, 'c': 3, 'd': 4}}
        self.ds['distance_read_key'] = [[0, 1], [1, 2], [3, 4]]

    def tearDown(self):
        from eskapade.core import execution
        execution.reset_eskapade()

    @mock.patch('eskapade.visualization.vis_utils.plot_overlay_histogram')
    @mock.patch('eskapade.data_mimic.dm_vis_util.plot_heatmaps')
    def test_mimic_report(self, mock_heatmaps, mock_overlay_hist):

        mock_heatmaps.return_value = True
        mock_overlay_hist.return_value = True

        link = data_mimic.links.MimicReport(
            read_key='read_key',
            resample_read_key='resample_read_key',
            store_key='store_key',
            new_column_order_read_key='new_column_order_read_key',
            results_path='results_path',
            chi2_read_key='chi2_read_key',
            p_value_read_key='p_value_read_key',
            maps_read_key='maps_read_key',
            key_data_normalized='key_data_normalized',
            distance_read_key='distance_read_key',
            corr_read_key='corr_read_key')
        link.initialize()
        link.execute()

        # should be called once
        assert mock_heatmaps.call_count == 1
        # should be called as many times as there are columns
        assert mock_overlay_hist.call_count == len(self.ds['continuous_i']) + \
            len(self.ds['ordered_categorical_i']) + \
            len(self.ds['unordered_categorical_i'])


class TestResampleEvaluation(unittest.TestCase):

    def setUp(self):
        from eskapade import process_manager, DataStore

        self.ds = process_manager.service(DataStore)

        self.ds['data'] = np.hstack((np.random.normal(0, 1, (100, 1)),
                                     np.random.normal(0, 1, (100, 1)),
                                     np.random.normal(0, 1, (100, 1)),
                                     np.random.randint(0, 4, (100, 1))))
        self.ds['data_resample'] = np.hstack((np.random.normal(0, 1, (100, 1)),
                                              np.random.normal(0, 1, (100, 1)),
                                              np.random.normal(0, 1, (100, 1)),
                                              np.random.randint(0, 4, (100, 1))))

        self.ds['bins'] = [np.array([0, 0.2, 0.6, 1]), np.array([0, 0.2, 0.6, 1]), np.array([0, 0.2, 0.6, 1]),
                           np.array([0, 0.2, 0.6, 1])]
        self.ds['n_bins'] = 2,
        self.ds['new_column_order'] = ['a', 'b', 'c', 'd']
        self.ds['df_resample'] = pd.DataFrame(self.ds['data_resample'],
                                              columns=self.ds['new_column_order'])
        self.ds['df_resample']['ID'] = np.random.randint(0, 100, 100)

    def tearDown(self):
        from eskapade.core import execution
        execution.reset_eskapade()

    @mock.patch('eskapade.data_mimic.data_mimic_util.scaled_chi')
    @mock.patch('scipy.stats.ks_2samp')
    @mock.patch('eskapade.analysis.correlation.calculate_correlations')
    def test_evaluater(self, mock_correlation, mock_ks, mock_chi):

        mock_correlation.return_value = 1
        mock_ks.return_value = (5, 500)
        mock_chi.return_value = (9999, -1)

        evaluater = data_mimic.ResampleEvaluation(data_read_key='data',
                                                  resample_read_key='data_resample',
                                                  bins=self.ds['bins'],
                                                  n_bins=2**7,
                                                  chi2_store_key='chi2',
                                                  p_value_store_key='p_value',
                                                  new_column_order_read_key='new_column_order',
                                                  ks_store_key='kss',
                                                  chis_store_key='chis',
                                                  distance_store_key='distance',
                                                  df_resample_read_key='df_resample',
                                                  corr_store_key='correlations')
        evaluater.initialize()
        evaluater.execute()

        assert mock_correlation.call_count == 2, f"Called {mock_correlation.call_count} times not 2"
        assert mock_ks.call_count == len(self.ds['new_column_order']), f"Called {mock_ks.call_count} times not 4"
        assert mock_chi.call_count == len(self.ds['new_column_order'])*(len(self.ds['new_column_order']) -1) + \
            len(self.ds['new_column_order']) + 1

        self.assertEqual(self.ds['kss'], {'a': {'ks': 5, 'p-value': 500},
                                          'b': {'ks': 5, 'p-value': 500},
                                          'c': {'ks': 5, 'p-value': 500},
                                          'd': {'ks': 5, 'p-value': 500}}), f"self.ds['kss']"

        self.assertEqual(self.ds['correlations'], [1, 1])

        self.assertIsNotNone(self.ds['chi2'])
        self.assertIsNotNone(self.ds['p_value'])
        self.assertIsNotNone(self.ds['kss'])
        self.assertIsNotNone(self.ds['chis'])
        self.assertIsNotNone(self.ds['distance'])
        self.assertIsNotNone(self.ds['correlations'])

    # def test_scaled_chi(self):

        
