import os
import pandas as pd

from eskapade.tests.integration.test_tutorial_macros import TutorialMacrosTest
from eskapade.core import execution, definitions
from eskapade import ProcessManager, ConfigObject, DataStore


class VisualizationTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on visualization tutorial macros"""

    def test_esk301(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk301_read_big_data_itr.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertTrue('test2' in ds)
        self.assertEqual(12, ds['n_test1'])
        self.assertEqual(2, ds['n_test2'])
        self.assertEqual(36, ds['n_sum_test1'])
        self.assertEqual(36, ds['n_sum_test2'])
        self.assertEqual(24, ds['n_merged'])
        
    def test_esk302(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk302_dfsummary_plotter.py'
        settings['batchMode'] = True

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)
        columns = ['var_a', 'var_b', 'var_c']

        # data-generation checks
        self.assertTrue(status.isSuccess())
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

    def test_esk303(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk303_histogram_filler_plotter.py'
        settings['batchMode'] = True

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)
        columns = ['date','isActive','age','eyeColor','gender','company','latitude','longitude']

        # data-generation checks
        self.assertTrue(status.isSuccess())
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

    def test_esk304(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk304_hgr_filler_plotter.py'
        settings['batchMode'] = True

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        # data-generation checks
        self.assertTrue(status.isSuccess())
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
