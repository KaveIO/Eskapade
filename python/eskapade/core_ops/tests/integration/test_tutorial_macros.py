import sys
import os
import importlib
import unittest
import unittest.mock as mock

from eskapade.tests.integration.test_bases import TutorialMacrosTest
import eskapade.utils
from eskapade.core import execution, definitions, persistence
from eskapade import ProcessManager, ConfigObject, DataStore
from eskapade.core_ops import Break


class CoreOpsTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on core-ops tutorial macros"""

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

    # TODO (janos4276): Ugh ... Fix this or remove this. This test relies on the old way of doing things.
    @unittest.skip('Fix or remove this test. This test relies on the old way of doing things!')
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
