import unittest
import os
import shutil
import pandas as pd

from eskapade.core import execution, definitions
from eskapade import ProcessManager, ConfigObject, DataStore


class TutorialMacrosTest(unittest.TestCase):

    def setUp(self):
        execution.reset_eskapade()

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
        settings['cmd'] = 'do_chain0=False'
        
        status = execution.run_eskapade(settings)

        pm = ProcessManager()
        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)


        self.assertTrue(status.isSuccess())
        self.assertEqual(1, len(pm.chains))
        self.assertEqual('Chain1', pm.chains[0].name)
        self.assertEqual(False, settings['do_chain0'])
        self.assertEqual(True, settings['do_chain1'])
        self.assertEqual('Universe', pm.chains[0].links[0].hello)

    def test_esk107(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk107_chain_looper.py'

        # fake a setting from the cmd-line. picked up in the macro
        settings['cmd'] = 'do_chain0=False'
        
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

        from eskapade.core_ops import BreakLink
        self.assertTrue(isinstance(pm.chains[0].links[2],BreakLink))
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

    def test_esk301(self):
        settings = ProcessManager().service(ConfigObject)
        settings['logLevel'] = definitions.LOG_LEVELS['DEBUG']
        settings['macro'] = settings['esRoot'] + '/tutorials/esk301_readdata_itr.py'

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
        self.assertEqual(36, ds['n_merged'])
        
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

    def tearDown(self):
        settings = ProcessManager().service(ConfigObject)
        execution.reset_eskapade()
        path = settings['resultsDir'] + '/' + settings['analysisName'] 
        if os.path.exists(path):
            shutil.rmtree(path)
