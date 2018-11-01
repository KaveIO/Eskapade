import numpy as np
import pandas as pd
import unittest
import unittest.mock as mock

from eskapade import data_mimic


class TestMimicReport(unittest.TestCase):

    def setUp(self):
        np.random.seed(42)

        from eskapade import process_manager, DataStore
        self.ds = process_manager.service(DataStore)

        # -- make fake data
        self.ds['new_column_order'] = ['a', 'b', 'c', 'd']
        self.ds['read_key'] = pd.DataFrame(np.hstack((np.random.normal(0, 1, (100, 1)),
                                                      np.random.normal(0, 1, (100, 1)),
                                                      np.random.normal(0, 1, (100, 1)),
                                                      np.random.randint(0, 4, (100, 1)))),
                                           columns=self.ds['new_column_order'])

        self.ds['resample_read_key'] = pd.DataFrame(np.hstack((np.random.normal(0, 1, (100, 1)),
                                                               np.random.normal(0, 1, (100, 1)),
                                                               np.random.normal(0, 1, (100, 1)),
                                                               np.random.randint(0, 4, (100, 1)))),
                                                    columns=self.ds['new_column_order'])

        self.ds['results_path'] = 1
        self.ds['chi2_read_key'] = 22
        self.ds['p_value_read_key'] = ''
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
            new_column_order_read_key='new_column_order',
            results_path='results_path',
            chi2_read_key='chi2_read_key',
            p_value_read_key='p_value_read_key',
            key_data_normalized='key_data_normalized',
            distance_read_key='distance_read_key',
            corr_read_key='corr_read_key',
            continuous_columns=['a', 'b', 'c'],
            ordered_categorical_columns=['d'],
            unordered_categorical_columns=[])
        link.initialize()
        link.execute()

        # should be called once
        assert mock_heatmaps.call_count == 1
        # should be called as many times as there are columns
        assert mock_overlay_hist.call_count == 2 * len(self.ds['continuous_i']) + \
            len(self.ds['ordered_categorical_i']) + \
            len(self.ds['unordered_categorical_i'])
