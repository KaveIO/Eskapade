import unittest
import mock
import sys
import os
import shutil
import importlib
import pandas as pd

import ROOT

import eskapade.utils
from eskapade.core import execution, definitions, persistence
from eskapade import ProcessManager, ConfigObject, DataStore
from eskapade.root_analysis import RooFitManager
from eskapade.core_ops import Break


class TutorialMacrosTest(unittest.TestCase):
    """Integration tests based on tutorial macros"""

    maxDiff = None

    def setUp(self):
        """Set up test"""

        execution.reset_eskapade()
        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        settings['analysisName'] = 'TutorialMacrosTest'
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['batchMode'] = True

    def run_eskapade(self, macro):
        """Run Eskapade"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        settings['macro'] = persistence.io_path('macros', settings.io_conf(), macro)
        status = execution.run_eskapade(settings)
        self.assertTrue(status.isSuccess())

    def test_esk101(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk101_helloworld.py'

        status = execution.run_eskapade(settings)

        settings = ProcessManager().service(ConfigObject)

        self.assertTrue(status.isSuccess())
        self.assertTrue(settings['do_hello'])
        self.assertEqual(2, settings['n_repeat'])

    def test_esk102(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk102_multiple_chains.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        self.assertTrue(status.isSuccess())
        self.assertTrue(settings['do_chain0'])
        self.assertTrue(settings['do_chain1'])
        self.assertTrue(settings['do_chain2'])
        self.assertEqual(3, len(pm.chains))

    def test_esk103(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk103_printdatastore.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertEqual('world', ds['hello'])
        self.assertEqual(1, ds['d']['a'])
        self.assertEqual(2, ds['d']['b'])
        self.assertEqual(3, ds['d']['c'])

    def test_esk104(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk104_basic_datastore_operations.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertEqual(1, len(ds.keys()))
        self.assertEqual(1, ds['a'])

    def test_esk105a(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk105_A_dont_store_results.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        path = settings['resultsDir'] + '/' + settings['analysisName'] 
        self.assertFalse(os.path.exists(path))
        
    def test_esk105bc(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk105_B_store_each_chain.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        # results of all three chains have been persisted 
        self.assertTrue(status.isSuccess())
        path = '{0:s}/{1:s}/proc_service_data/v0/_chain{{:d}}/{2:s}.pkl'.format(
            settings['resultsDir'], settings['analysisName'], str(DataStore))
        for path_it in range(1, 4):
            self.assertTrue(os.path.exists(path.format(path_it)))

        execution.reset_eskapade()

        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk105_C_begin_at_chain3.py'

        status = execution.run_eskapade(settings)

        ds = ProcessManager().service(DataStore)


        # object from all three chains are present
        self.assertTrue(status.isSuccess())
        self.assertTrue('f' in ds)
        self.assertTrue('g' in ds)
        self.assertTrue('h' in ds)
        self.assertEqual(3, len(ds.keys()))
        self.assertEqual(7, ds['f']['n_favorite'])
        self.assertEqual(1, ds['g']['a'])
        self.assertEqual(7, ds['h'][1])

    def test_esk106(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk106_cmdline_options.py'

        # fake a setting from the cmd-line. picked up in the macro
        settings['do_chain0'] = False
        
        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertEqual(1, len(pm.chains))
        self.assertEqual('Chain1', pm.chains[0].name)
        self.assertEqual(False, settings.get('do_chain0', True))
        self.assertEqual(True, settings.get('do_chain1', True))
        self.assertEqual('Universe', pm.chains[0].links[0].hello)

    @mock.patch('sys.argv')
    def test_esk106_script(self, mock_argv):
        """Test Eskapade run with esk106 macro from script"""

        proc_mgr = ProcessManager()

        # get file paths
        settings = proc_mgr.service(ConfigObject)
        settings['analysisName'] = 'esk106_cmdline_options'
        settings_ = settings.copy()
        script_path = eskapade.utils.get_file_path('run_eskapade')
        macro_path = persistence.io_path('macros', settings.io_conf(), 'esk106_cmdline_options.py')

        # import run-script module
        orig_mod_path = sys.path.copy()
        sys.path.append(os.path.dirname(script_path))
        script_mod = os.path.splitext(os.path.basename(script_path))[0]
        run_eskapade = importlib.import_module(script_mod)

        # mock command-line arguments
        args = []
        mock_argv.__getitem__ = lambda s, k: args.__getitem__(k)

        # base settings
        args_ = [script_path, macro_path, '-LDEBUG', '--batch-mode']
        settings_['macro'] = macro_path
        settings_['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings_['batchMode'] = True

        def do_run(name, args, args_, settings_, add_args, add_settings, chains):
            # set arguments
            args.clear()
            args += args_ + add_args
            settings = settings_.copy()
            settings.update(add_settings)

            # run Eskapade
            proc_mgr.reset()
            run_eskapade.main()
            settings_run = proc_mgr.service(ConfigObject)

            # check results
            self.assertListEqual([c.name for c in proc_mgr.chains], chains,
                                 'unexpected chain names in "{}" test'.format(name))
            self.assertDictEqual(settings_run, settings, 'unexpected settings in "{}" test'.format(name))

        # run both chains
        do_run('both chains', args, args_, settings_,
               ['--store-all', '-cdo_chain0=True', '-cdo_chain1=True'],
               dict(storeResultsEachChain=True, do_chain0=True, do_chain1=True),
               ['Chain0', 'Chain1'])

        # run only last chain by skipping the first
        do_run('skip first', args, args_, settings_,
               ['-bChain1', '-cdo_chain0=True', '-cdo_chain1=True'],
               dict(beginWithChain='Chain1', do_chain0=True, do_chain1=True),
               ['Chain0', 'Chain1'])

        # run only last chain by not defining the first
        do_run('no first', args, args_, settings_,
               ['-cdo_chain0=False', '-cdo_chain1=True'],
               dict(do_chain0=False, do_chain1=True),
               ['Chain1'])

        # restore module search path
        sys.path.clear()
        sys.path += orig_mod_path

    def test_esk107(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk107_chain_looper.py'
        
        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        # chain is repeated 10 times, with nothing put in datastore
        self.assertTrue(status.isSuccess())
        self.assertEqual(0, len(ds.keys()))
        self.assertEqual(10, pm.chains[0].links[1].maxcount)

    def test_esk108map(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk108_map.py'
        settings['TESTING'] = True
        
        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        
    def test_esk108reduce(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk108_reduce.py'
        settings['TESTING'] = True
        
        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        self.assertTrue(status.isSuccess())
        self.assertEqual(20, ds['n_products'])
        
    def test_esk109(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk109_debugging_tips.py'

        # this flag turns off ipython embed link
        settings['TESTING'] = True

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        self.assertTrue(isinstance(pm.chains[0].links[2], Break))
        self.assertTrue(status.isFailure())

    def test_esk110(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk110_code_profiling.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        self.assertTrue(status.isSuccess())
        self.assertEqual(0, len(pm.chains))
        self.assertEqual(0, len(ds.keys()))
        self.assertTrue('doCodeProfiling' in settings)
        self.assertEqual('cumulative', settings['doCodeProfiling'])

    def test_esk201(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk201_readdata.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertTrue('test1' in ds)
        self.assertTrue('test2' in ds)
        self.assertEqual(12, ds['n_test1'])
        self.assertEqual(36, ds['n_test2'])

    def test_esk202(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk202_writedata.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        self.assertTrue(status.isSuccess())
        self.assertEqual(36, ds['n_test'])
        path = settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/tmp3.csv'
        self.assertTrue(os.path.exists(path))
        # check file is non-empty
        statinfo = os.stat(path)
        self.assertTrue(statinfo.st_size > 0)
        
    def test_esk203(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk203_apply_func_to_pandas_df.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        self.assertTrue(status.isSuccess())
        self.assertTrue('transformed_data' in ds)
        df = ds['transformed_data']
        self.assertTrue('xx' in df.columns)
        self.assertTrue('yy' in df.columns)

    def test_esk204(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk204_apply_query_to_pandas_df.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertTrue('outgoing_records' in ds)
        self.assertTrue(ds['n_outgoing_records']>0)
        df = ds['outgoing_records']
        self.assertTrue('a' in df.columns)
        self.assertFalse('b' in df.columns)
        self.assertTrue('c' in df.columns)

    def test_esk205(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk205_concatenate_pandas_dfs.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertTrue('outgoing' in ds)
        self.assertEqual(ds['n_outgoing'],12)

    def test_esk206(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk206_merge_pandas_dfs.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertTrue('outgoing' in ds)
        df = ds['outgoing']
        self.assertEqual(len(df.index),4)
        self.assertEqual(len(df.columns),5)

    def test_esk207(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk207_record_vectorizer.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        columns = sorted(['x_1', 'x_3', 'x_5', 'x_4', 'y_9', 'y_8', 'y_7', 'y_6', 'y_5', 'y_4'])

        self.assertTrue(status.isSuccess())
        self.assertTrue('vect_test' in ds)
        df = ds['vect_test']
        self.assertEqual(len(df.index),12)
        self.assertListEqual(sorted(df.columns.tolist()), columns)

    def test_esk208(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk208_record_factorizer.py'

        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        self.assertTrue(status.isSuccess())
        self.assertTrue('test1' in ds)
        self.assertTrue('test1_fact' in ds)
        self.assertTrue('test1_refact' in ds)
        self.assertTrue('to_original' in ds)
        df1 = ds['test1']
        df2 = ds['test1_refact']
        self.assertEqual(len(df1.index),12)
        self.assertEqual(len(df2.index),12)
        self.assertTrue('dummy' in df1.columns)
        self.assertTrue('loc' in df1.columns)
        self.assertTrue('dummy' in df2.columns)
        self.assertTrue('loc' in df2.columns)
        self.assertListEqual(df1['dummy'].values.tolist(), df2['dummy'].values.tolist())
        self.assertListEqual(df1['loc'].values.tolist(), df2['loc'].values.tolist())

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

    def test_esk401(self):
        """Test Esk-401: ROOT hist fill, plot, convert"""

        # run Eskapade
        self.run_eskapade('esk401_roothist_fill_plot_convert.py')
        ds = ProcessManager().service(DataStore)

        # histogram checks
        self.assertIn('hist', ds)
        self.assertIsInstance(ds['hist'], dict)
        columns = ['x1', 'x2', 'x3', 'x4', 'x5', 'x1:x2', 'x2:x3', 'x4:x5']
        self.assertListEqual(sorted(ds['hist'].keys()), sorted(columns))
        for col in columns:
            self.assertIsInstance(ds['hist'][col], ROOT.TH1)

        # data-generation checks
        self.assertIn('n_correlated_data', ds)
        self.assertEqual(500, ds['n_correlated_data'])
        self.assertIn('n_rdh_x1', ds)
        self.assertEqual(100, ds['n_rdh_x1'])
        self.assertIn('n_rds_x2_vs_x3', ds)
        self.assertEqual(114, ds['n_rds_x2_vs_x3'])

        # roofit objects check
        self.assertIn('hpdf', ds)
        self.assertIsInstance(ds['hpdf'], ROOT.RooHistPdf)
        self.assertIn('rdh_x1', ds)
        self.assertIsInstance(ds['rdh_x1'], ROOT.RooDataHist)
        self.assertIn('rds_x2_vs_x3', ds)
        self.assertIsInstance(ds['rds_x2_vs_x3'], ROOT.RooDataSet)
        self.assertIn('vars_x2_vs_x3', ds)
        self.assertIsInstance(ds['vars_x2_vs_x3'], ROOT.RooArgSet)

        # data-summary checks
        io_conf = ProcessManager().service(ConfigObject).io_conf()
        file_names = ['report.tex'] + ['hist_{}.pdf'.format(col.replace(':','_vs_')) for col in columns]
        for fname in file_names:
            path = persistence.io_path('results_data', io_conf, 'report/{}'.format(fname))
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk402(self):
        """Test Esk-402: RooDataHist fill"""

        # run Eskapade
        self.run_eskapade('esk402_roodatahist_fill.py')
        ds = ProcessManager().service(DataStore)

        # data-generation checks
        self.assertIn('n_accounts', ds)
        self.assertEqual(650, ds['n_accounts'])
        self.assertIn('n_rdh_accounts', ds)
        self.assertEqual(650, ds['n_rdh_accounts'])
        self.assertIn('to_factorized', ds)
        self.assertIsInstance(ds['to_factorized'], dict)
        self.assertIn('to_original', ds)
        self.assertIsInstance(ds['to_original'], dict)
        self.assertIn('map_rdh_accounts_to_original', ds)
        self.assertIsInstance(ds['map_rdh_accounts_to_original'], dict)

        # roofit objects check
        self.assertIn('accounts_catset', ds)
        self.assertIsInstance(ds['accounts_catset'], ROOT.RooArgSet)
        self.assertEqual(2, len(ds['accounts_catset']))
        self.assertIn('accounts_varset', ds)
        self.assertIsInstance(ds['accounts_varset'], ROOT.RooArgSet)
        self.assertEqual(6, len(ds['accounts_varset']))
        self.assertIn('rdh_accounts', ds)
        self.assertIsInstance(ds['rdh_accounts'], ROOT.RooDataHist)

    def test_esk403(self):
        """Test Esk-403: RooDataSet convert"""

        # run Eskapade
        self.run_eskapade('esk403_roodataset_convert.py')
        ds = ProcessManager().service(DataStore)

        # data-generation checks
        self.assertIn('n_accounts', ds)
        self.assertEqual(650, ds['n_accounts'])
        self.assertIn('n_rds_accounts', ds)
        self.assertEqual(650, ds['n_rds_accounts'])
        self.assertIn('n_df_from_rds', ds)
        self.assertEqual(650, ds['n_df_from_rds'])

        self.assertIn('to_factorized', ds)
        self.assertIsInstance(ds['to_factorized'], dict)
        self.assertIn('to_original', ds)
        self.assertIsInstance(ds['to_original'], dict)
        self.assertIn('rds_to_original', ds)
        self.assertIsInstance(ds['rds_to_original'], dict)

        # roofit objects check
        self.assertIn('accounts_fact_varset', ds)
        self.assertIsInstance(ds['accounts_fact_varset'], ROOT.RooArgSet)
        self.assertEqual(4, len(ds['accounts_fact_varset']))
        self.assertIn('rds_accounts', ds)
        self.assertIsInstance(ds['rds_accounts'], ROOT.RooDataSet)

        # assert that refactored df equals original
        self.assertIn('accounts', ds)
        self.assertIn('df_refact', ds)
        df1 = ds['accounts']
        df2 = ds['df_refact']
        self.assertEqual(len(df1.index),650)
        self.assertEqual(len(df2.index),650)
        self.assertTrue('eyeColor' in df1.columns)
        self.assertTrue('favoriteFruit' in df1.columns)
        self.assertTrue('eyeColor' in df2.columns)
        self.assertTrue('favoriteFruit' in df2.columns)
        self.assertListEqual(df1['eyeColor'].values.tolist(), df2['eyeColor'].values.tolist())
        self.assertListEqual(df1['favoriteFruit'].values.tolist(), df2['favoriteFruit'].values.tolist())

    def test_esk404(self):
        """Test Esk-404: Workspace create PDF, simulate, fit, plot"""

        # run Eskapade
        self.run_eskapade('esk404_workspace_createpdf_simulate_fit_plot.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # data-generation checks
        self.assertIn('n_df_simdata', ds)
        self.assertEqual(1000, ds['n_df_simdata'])

        # roofit objects check in datastore
        self.assertIn('fit_result', ds)
        self.assertIsInstance(ds['fit_result'], ROOT.RooFitResult)

        # successful fit result
        fit_result = ds['fit_result']
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        self.assertIn('simdata', ds)
        self.assertIsInstance(ds['simdata'], ROOT.RooDataSet)
        self.assertIn('simdata_plot', ds)
        self.assertIsInstance(ds['simdata_plot'], ROOT.RooPlot)

        # roofit objects check in workspace
        self.assertIn('model', ws)
        self.assertIn('bkg', ws)
        self.assertIn('sig', ws)

    def test_esk405(self):
        """Test Esk-405: Simulation based on binned data"""

        # run Eskapade
        self.run_eskapade('esk405_simulation_based_on_binned_data.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # data-generation checks
        self.assertIn('n_rdh_accounts', ds)
        self.assertEqual(650, ds['n_rdh_accounts'])

        # roofit objects check in workspace
        self.assertIn('hpdf_Ndim', ws)
        self.assertIn('rdh_accounts', ws)

        mcats = ws.set('rdh_cats')
        self.assertFalse(not mcats)
        self.assertEqual(1, len(mcats))
        mvars = ws.set('rdh_vars')
        self.assertFalse(not mvars)
        self.assertEqual(3, len(mvars))
        mdata = ws.data('rdh_accounts')
        self.assertEqual(650, mdata.sumEntries())

    def test_esk406(self):
        """Test Esk-406: Simulation based on unbinned data"""

        # run Eskapade
        self.run_eskapade('esk406_simulation_based_on_unbinned_data.py')
        ds = ProcessManager().service(DataStore)

        # data-generation checks
        self.assertIn('n_correlated_data', ds)
        self.assertEqual(500, ds['n_correlated_data'])
        self.assertIn('n_rds_correlated_data', ds)
        self.assertEqual(500, ds['n_rds_correlated_data'])
        self.assertIn('n_df_simdata', ds)
        self.assertEqual(5000, ds['n_df_simdata'])

        self.assertIn('df_simdata', ds)
        self.assertIsInstance(ds['df_simdata'], pd.DataFrame)
        self.assertIn('hist', ds)
        self.assertIsInstance(ds['hist'], dict)

        # roofit objects check
        self.assertIn('keys_varset', ds)
        self.assertIsInstance(ds['keys_varset'], ROOT.RooArgSet)
        self.assertEqual(2, len(ds['keys_varset']))
        self.assertIn('rds_correlated_data', ds)
        self.assertIsInstance(ds['rds_correlated_data'], ROOT.RooDataSet)
        self.assertIn('simdata', ds)
        self.assertIsInstance(ds['simdata'], ROOT.RooDataSet)

    def test_esk407(self):
        """Test Esk-407: Classification unbiased fit estimate"""

        # run Eskapade
        self.run_eskapade('esk407_classification_unbiased_fit_estimate.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # roofit objects check in datastore
        self.assertIn('fit_result', ds)
        self.assertIsInstance(ds['fit_result'], ROOT.RooFitResult)

        # roofit objects check in workspace
        mdata = ws.data('data')
        self.assertFalse(not mdata)
        self.assertEqual(1000, mdata.numEntries())
        mpdf = ws.pdf('hist_model')
        self.assertFalse(not mpdf)

        # successful fit result
        fit_result = ds['fit_result']
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        lo_risk = ws.var('N_low_risk')
        self.assertFalse(not lo_risk)
        self.assertTrue(lo_risk.getVal()<1000)
        self.assertTrue(lo_risk.getError()>0)
        hi_risk = ws.var('N_high_risk')
        self.assertFalse(not hi_risk)
        self.assertTrue(hi_risk.getVal()>0)
        self.assertTrue(hi_risk.getError()>0)

    def test_esk408(self):
        """Test Esk-408: Classification error propagation after fit"""

        # run Eskapade
        self.run_eskapade('esk408_classification_error_propagation_after_fit.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # data-generation checks
        self.assertIn('n_df_pvalues', ds)
        self.assertEqual(500, ds['n_df_pvalues'])
        self.assertIn('df_pvalues', ds)
        self.assertIsInstance(ds['df_pvalues'], pd.DataFrame)
        df = ds['df_pvalues']
        self.assertTrue('high_risk_pvalue' in df.columns)
        self.assertTrue('high_risk_perror' in df.columns)

        # roofit objects check in workspace
        fit_result = ws.obj('fit_result')
        self.assertFalse(not fit_result)
        self.assertIsInstance(fit_result, ROOT.RooFitResult)
        # test for successful fit result
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        frac = ws.var('frac')
        self.assertFalse(not frac)
        self.assertTrue(frac.getVal()>0)
        self.assertTrue(frac.getError()>0)

    def test_esk409(self):
        """Test Esk-409: Unredeemed vouchers"""

        # run Eskapade
        self.run_eskapade('esk409_unredeemed_vouchers.py')
        proc_mgr = ProcessManager()
        ds = proc_mgr.service(DataStore)

        # check generated data
        self.assertIn('voucher_redeems', ds)
        self.assertIn('voucher_ages', ds)
        self.assertIsInstance(ds['voucher_redeems'], ROOT.RooDataSet)
        self.assertIsInstance(ds['voucher_ages'], ROOT.RooDataSet)
        self.assertLess(ds['voucher_redeems'].numEntries(), 6000)
        self.assertGreater(ds['voucher_redeems'].numEntries(), 0)
        self.assertEqual(ds['voucher_ages'].numEntries(), 10000)

        # check fit result
        fit_link = proc_mgr.get_chain('Fitting').get_link('Fit')
        self.assertEqual(fit_link.fit_result.status(), 0)
        n_ev_pull = (fit_link.results['n_ev'][0] - 6000.) / fit_link.results['n_ev'][1]
        self.assertGreater(n_ev_pull, -3.)
        self.assertLess(n_ev_pull, 3.)

        # check plot output
        plot_path = persistence.io_path('results_data', proc_mgr.service(ConfigObject).io_conf(), 'voucher_redeem.pdf')
        self.assertTrue(os.path.exists(plot_path))
        statinfo = os.stat(plot_path)
        self.assertGreater(statinfo.st_size, 0)

    def tearDown(self):
        """Tear down test"""

        # reset run process
        settings = ProcessManager().service(ConfigObject)
        execution.reset_eskapade()
        if not settings.get('analysisName'):
            return

        # remove persisted results for this test
        path = persistence.io_dir('ana_results', settings.io_conf())
        if os.path.exists(path):
            shutil.rmtree(path)
