import importlib
import os
import sys
import unittest
import unittest.mock as mock

import eskapade.utils
from eskapade import process_manager, resources, ConfigObject, DataStore, StatusCode, entry_points
from eskapade.core import execution
from eskapade.core_ops import Break
from eskapade.logger import LogLevel
from eskapade_python.bases import TutorialMacrosTest


class CoreOpsTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on core-ops tutorial macros"""

    def setUp(self):
        """Set up test"""

        TutorialMacrosTest.setUp(self)
        settings = process_manager.service(ConfigObject)
        settings['analysisName'] = 'CoreOpsTutorialMacrosTest'

    def test_esk101(self):
        self.eskapade_run(resources.tutorial('esk101_helloworld.py'))

        settings = process_manager.service(ConfigObject)

        self.assertTrue(settings['do_hello'])
        self.assertEqual(2, settings['n_repeat'])

    def test_esk102(self):
        self.eskapade_run(resources.tutorial('esk102_multiple_chains.py'))

        settings = process_manager.service(ConfigObject)

        self.assertTrue(settings['do_chain0'])
        self.assertTrue(settings['do_chain1'])
        self.assertTrue(settings['do_chain2'])
        self.assertEqual(3, len(process_manager))

    def test_esk103(self):
        self.eskapade_run(resources.tutorial('esk103_printdatastore.py'))

        ds = process_manager.service(DataStore)

        self.assertEqual('world', ds['hello'])
        self.assertEqual(1, ds['d']['a'])
        self.assertEqual(2, ds['d']['b'])
        self.assertEqual(3, ds['d']['c'])

    def test_esk104(self):
        self.eskapade_run(resources.tutorial('esk104_basic_datastore_operations.py'))

        ds = process_manager.service(DataStore)

        self.assertEqual(1, len(ds))
        self.assertEqual(1, ds['a'])

    def test_esk105a(self):
        self.eskapade_run(resources.tutorial('esk105_A_dont_store_results.py'))

        settings = process_manager.service(ConfigObject)
        path = settings['resultsDir'] + '/' + settings['analysisName']

        self.assertFalse(os.path.exists(path))

    def test_esk105bc(self):
        self.eskapade_run(resources.tutorial('esk105_B_store_each_chain.py'))

        settings = process_manager.service(ConfigObject)
        # results of all three chains have been persisted
        path = '{0:s}/{1:s}/proc_service_data/v0/_chain{{:d}}/{2:s}.pkl'.format(
            settings['resultsDir'], settings['analysisName'], str(DataStore))

        for path_it in range(1, 4):
            self.assertTrue(os.path.exists(path.format(path_it)))

        execution.reset_eskapade()
        self.eskapade_run(resources.tutorial('esk105_C_begin_at_chain3.py'))

        ds = process_manager.service(DataStore)

        # object from all three chains are present
        self.assertTrue('f' in ds)
        self.assertTrue('g' in ds)
        self.assertTrue('h' in ds)
        self.assertEqual(3, len(ds))
        self.assertEqual(7, ds['f']['n_favorite'])
        self.assertEqual(1, ds['g']['a'])
        self.assertEqual(7, ds['h'][1])

    def test_esk106(self):
        settings = process_manager.service(ConfigObject)
        # fake a setting from the cmd-line. picked up in the macro
        settings['do_chain0'] = False

        self.eskapade_run(resources.tutorial('esk106_cmdline_options.py'))

        settings = process_manager.service(ConfigObject)

        self.assertEqual(1, len(process_manager))
        self.assertEqual('Chain1', list(process_manager)[0].name)
        self.assertEqual(False, settings.get('do_chain0', True))
        self.assertEqual(True, settings.get('do_chain1', True))
        self.assertEqual('Universe', list(list(process_manager)[0])[0].hello)

    @unittest.skip('TODO: Rewrite this test! It is supposed to test argument passing to Eskapade via the command '
                   'line, which is currently not working. For some reason the analysis name is not picked up from the'
                   ' macro. We could look at the eskapade_bootstrap test for inspiration.')
    @mock.patch('sys.argv')
    def test_esk106_script(self, mock_argv):
        """Test Eskapade run with esk106 macro from script"""

        # get file paths
        settings = process_manager.service(ConfigObject)
        settings['analysisName'] = 'esk106_cmdline_options'
        settings_ = settings.copy()
        macro_path = resources.tutorial('esk106_cmdline_options.py')

        # mock command-line arguments
        args = []
        mock_argv.__getitem__ = lambda s, k: args.__getitem__(k)

        # base settings
        args_ = [macro_path, '-LDEBUG', '--batch-mode']
        settings_['macro'] = macro_path
        settings_['logLevel'] = LogLevel.DEBUG
        settings_['batchMode'] = True

        def do_run(name, args, args_, settings_, add_args, add_settings, chains):
            # set arguments
            args.clear()
            args += args_ + add_args
            settings = settings_.copy()
            settings.update(add_settings)

            # run Eskapade
            process_manager.reset()
            entry_points.eskapade_run()
            settings_run = process_manager.service(ConfigObject)

            # check results
            self.assertListEqual([c.name for c in process_manager.chains], chains,
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

    def test_esk107(self):
        self.eskapade_run(resources.tutorial('esk107_chain_looper.py'))

        ds = process_manager.service(DataStore)

        # chain is repeated 10 times, with nothing put in datastore
        self.assertEqual(0, len(ds))
        self.assertEqual(10, list(list(process_manager)[0])[1].maxcount)

    def test_esk108map(self):
        settings = process_manager.service(ConfigObject)
        settings['TESTING'] = True

        self.eskapade_run(resources.tutorial('esk108_map.py'))

    def test_esk108reduce(self):
        settings = process_manager.service(ConfigObject)
        settings['TESTING'] = True
        self.eskapade_run(resources.tutorial('esk108_reduce.py'))

        ds = process_manager.service(DataStore)

        self.assertEqual(20, ds['n_products'])

    def test_esk109(self):
        settings = process_manager.service(ConfigObject)
        # this flag turns off ipython embed link
        settings['TESTING'] = True

        self.eskapade_run(resources.tutorial('esk109_debugging_tips.py'), StatusCode.Failure)

        self.assertTrue(isinstance(list(list(process_manager)[0])[2], Break))

    def test_esk110(self):
        self.eskapade_run(resources.tutorial('esk110_code_profiling.py'))

        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        self.assertEqual(0, len(process_manager))
        self.assertEqual(0, len(ds))
        self.assertTrue('doCodeProfiling' in settings)
        self.assertEqual('cumulative', settings['doCodeProfiling'])
