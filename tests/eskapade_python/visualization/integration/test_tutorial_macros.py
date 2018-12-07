import os

import pandas as pd

from eskapade import process_manager, resources, ConfigObject, DataStore
from escore.core import persistence
from eskapade_python.bases import TutorialMacrosTest


class VisualizationTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on visualization tutorial macros"""

    def test_esk301(self):
        settings = process_manager.service(ConfigObject)
        settings['batchMode'] = True

        self.eskapade_run(resources.tutorial('esk301_dfsummary_plotter.py'))

        settings = process_manager.service(ConfigObject)

        ds = process_manager.service(DataStore)

        columns = ['var_a', 'var_b', 'var_c']

        # data-generation checks
        self.assertIn('data', ds)
        self.assertIsInstance(ds['data'], pd.DataFrame)
        self.assertListEqual(list(ds['data'].columns), columns)
        self.assertEqual(10000, len(ds['data']))

        # data-summary checks
        file_names = ['report.tex'] + ['hist_{}.pdf'.format(col) for col in columns]
        for fname in file_names:
            path = '{0:s}/{1:s}/data/v0/report/{2:s}'.format(settings['resultsDir'], settings['analysisName'], fname)
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk302(self):
        settings = process_manager.service(ConfigObject)
        settings['batchMode'] = True

        self.eskapade_run(resources.tutorial('esk302_histogram_filler_plotter.py'))

        settings = process_manager.service(ConfigObject)

        ds = process_manager.service(DataStore)

        columns = ['date', 'isActive', 'age', 'eyeColor', 'gender', 'company', 'latitude', 'longitude']

        # data-generation checks
        self.assertIn('n_sum_rc', ds)
        self.assertEqual(1300, ds['n_sum_rc'])
        self.assertIn('hist', ds)
        self.assertIsInstance(ds['hist'], dict)
        self.assertListEqual(sorted(ds['hist'].keys()), sorted(columns))

        # data-summary checks
        file_names = ['report.tex'] + ['hist_{}.pdf'.format(col) for col in columns]
        for fname in file_names:
            path = '{0:s}/{1:s}/data/v0/report/{2:s}'.format(settings['resultsDir'], settings['analysisName'], fname)
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk303(self):
        settings = process_manager.service(ConfigObject)
        settings['batchMode'] = True

        self.eskapade_run(resources.tutorial('esk303_hgr_filler_plotter.py'))

        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # data-generation checks
        self.assertIn('n_sum_rc', ds)
        self.assertEqual(650, ds['n_sum_rc'])
        self.assertIn('hist', ds)
        self.assertIsInstance(ds['hist'], dict)
        col_names = ['date', 'isActive', 'age', 'eyeColor', 'gender', 'company', 'latitude', 'longitude',
                     'isActive:age', 'latitude:longitude']
        self.assertListEqual(sorted(ds['hist'].keys()), sorted(col_names))

        # data-summary checks
        f_bases = ['date', 'isActive', 'age', 'eyeColor', 'gender', 'company', 'latitude', 'longitude',
                   'latitude_vs_longitude']
        file_names = ['report.tex'] + ['hist_{}.pdf'.format(col) for col in f_bases]
        for fname in file_names:
            path = '{0:s}/{1:s}/data/v0/report/{2:s}'.format(settings['resultsDir'], settings['analysisName'], fname)
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk304(self):
        settings = process_manager.service(ConfigObject)
        settings['batchMode'] = True

        self.eskapade_run(resources.tutorial('esk304_df_boxplot.py'))

        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # data-generation checks
        self.assertIn('data', ds)
        self.assertIsInstance(ds['data'], pd.DataFrame)
        self.assertEqual(10000, len(ds['data']))
        self.assertListEqual(sorted(ds['data'].columns), ['var_a', 'var_b', 'var_c'])

        # data-summary checks
        file_names = ['report.tex', 'boxplot_var_a.pdf', 'boxplot_var_c.pdf']
        for fname in file_names:
            path = '{0:s}/{1:s}/data/v0/report/{2:s}'.format(settings['resultsDir'], settings['analysisName'], fname)
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk305(self):
        settings = process_manager.service(ConfigObject)
        settings['batchMode'] = True

        self.eskapade_run(resources.tutorial('esk305_correlation_summary.py'))

        ds = process_manager.service(DataStore)

        # input data checks
        all_col_names = ['x1', 'x2', 'x3', 'x4', 'x5', 'Unnamed: 5']

        self.assertIn('input_data', ds)
        self.assertIsInstance(ds['input_data'], pd.DataFrame)
        self.assertListEqual(list(ds['input_data'].columns), all_col_names)

        self.assertIn('correlations', ds)
        self.assertIsInstance(ds['correlations'], list)
        corr_list = ds['correlations']
        self.assertEqual(5, len(corr_list))

        # correlation matrix checks
        col_names = ['x1', 'x2', 'x3', 'x4', 'x5']

        for corr in corr_list:
            self.assertIsInstance(corr, pd.DataFrame)
            # self.assertListEqual(list(corr.columns), col_names)
            self.assertListEqual(list(corr.index), col_names)

        # heatmap pdf checks
        results_path = persistence.io_path('results_data', 'report')

        correlations = ['pearson', 'kendall', 'spearman', 'correlation_ratio', 'phik']
        for corr in correlations:
            path = '{0:s}/correlations_input_data_{1:s}.pdf'.format(results_path, corr)
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk306(self):
        settings = process_manager.service(ConfigObject)
        settings['batchMode'] = True

        self.eskapade_run(resources.tutorial('esk306_concatenate_reports.py'))

        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # report checks
        self.assertIn('report_pages', ds)
        self.assertIsInstance(ds['report_pages'], list)
        self.assertEqual(21, len(ds['report_pages']))

        # data-summary checks
        file_names = ['report.tex']
        for fname in file_names:
            path = '{0:s}/{1:s}/data/v0/report/{2:s}'.format(settings['resultsDir'], settings['analysisName'], fname)
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)
