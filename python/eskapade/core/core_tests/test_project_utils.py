import unittest

from ..project_utils import create_parser
from eskapade import ConfigObject

class CommandLineArgumentTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_create_parser_default(self):
        settings = ConfigObject()
        test_parser = create_parser(settings)
        test_args = test_parser.parse_args(["path"])

        # The dict is what is in the parser after parsing CLAs.
        self.assertDictEqual({'run_profiling': False, 'cmd': None,
                              'configFile': ['path'],
                              'userArg': '',
                              'interactive': False,
                              'single_chain': '',
                              'seed': 0,
                              'results_dir': '',
                              'macros_dir': '',
                              'store_intermediate_result': False,
                              'begin_with_chain': '',
                              'do_not_store_results': False,
                              'log_format': '%(asctime)s %(levelname)s [%(module)s/%(funcName)s]: %(message)s',
                              'log_level': None,
                              'end_with_chain': '',
                              'store_intermediate_result_one_chain': '',
                              'analysis_name': '',
                              'data_dir': '',
                              'unpickle_config': False,
                              'data_version': 0,
                              'batch_mode': True}, test_args.__dict__)

    def test_create_parser(self):
        settings = ConfigObject()
        settings['seed'] = 12345
        settings['batchMode'] = False
        settings['interactive'] = True
        settings['storeResultsEachChain'] = True
        settings['doNotStoreResults'] = True

        test_parser = create_parser(settings)
        test_args = test_parser.parse_args(["path", "-d=dir", "--cmd='a=1'", "-a analysis"])

        self.assertDictEqual({'run_profiling': False,
                              'cmd': "'a=1'",
                              'configFile': ['path'],
                              'userArg': '',
                              'interactive': True,
                              'single_chain': '',
                              'seed': 12345,
                              'results_dir': '',
                              'macros_dir': '',
                              'store_intermediate_result': True,
                              'begin_with_chain': '',
                              'do_not_store_results': True,
                              'log_format': '%(asctime)s %(levelname)s [%(module)s/%(funcName)s]: %(message)s',
                              'log_level': None,
                              'end_with_chain': '',
                              'store_intermediate_result_one_chain': '',
                              'analysis_name': ' analysis',
                              'data_dir': 'dir',
                              'unpickle_config': False,
                              'data_version': 0,
                              'batch_mode': False}, test_args.__dict__)


    def tearDown(self):
        pass
