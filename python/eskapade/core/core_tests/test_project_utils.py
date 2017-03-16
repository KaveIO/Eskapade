import unittest
import os
import argparse

from ..project_utils import create_parser, arg_setter
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


class ArgumentSetterTest(unittest.TestCase):
    # If you want to read the output of the unittest when it fails, uncomment the following line
    # maxDiff = None

    def setUp(self):
        try:
            self.pythonpath = os.environ['ESKAPADE']
            assert len(self.pythonpath) != 0, 'eskapade python path not set properly.'
        except:
            raise Exception('eskapade python path not set properly.')

    def test_arg_setter_default(self):
        settings = ConfigObject()

        parser = argparse.ArgumentParser()
        parser.add_argument("configFile", nargs="+", help="configuration file to execute")
        parser.add_argument("-L", "--log-level", help="set log level",
                            choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL", "OFF"])
        parser.add_argument("-F", "--log-format", help="format of log messages",
                            default="%(asctime)s %(levelname)s [%(module)s/%(funcName)s]: %(message)s")
        parser.add_argument("-S", "--seed", type=int, help="set the random seed for toy generation",
                            default=settings['seed'])
        parser.add_argument("-B", "--batch-mode", help="run in batch mode, not using X Windows",
                            action="store_true", default=False)
        parser.add_argument("-i", "--interactive", help="remain in interactive mode after running",
                            action="store_true", default=False)
        parser.add_argument("-b", "--begin-with-chain", help="begin running from particular chain in chain-list",
                            default="")
        parser.add_argument("-e", "--end-with-chain", help="last chain to run in chain-list", default="")
        parser.add_argument("-s", "--single-chain", help="select which single chain to run", default="")
        parser.add_argument("-w", "--store-intermediate-result",
                            help="store intermediate result after each chain, not only at end",
                            action="store_true", default=False)
        parser.add_argument("-W", "--store-intermediate-result-one-chain",
                            help="store intermediate result of one chain",
                            default="")
        parser.add_argument("-c", "--cmd", help="python commands to process (semi-colon-seperated)")
        parser.add_argument("-U", "--userArg", help="arbitrary user argument(s)", default="")
        parser.add_argument("-P", "--run-profiling",
                            help="Run a python profiler during main Eskapade execution",
                            action="store_true")
        parser.add_argument("-v", "--data-version", help="use the samples for training containing this version number",
                            type=int, default=0)
        parser.add_argument("-a", "--analysis-name", help="The name of the analysis", default="")
        parser.add_argument("-u", "--unpickle-config", help="Unpickle configuration object from configuration file.",
                            action="store_true", default=False)
        parser.add_argument("-r", "--results-dir", help="Set path of the storage results directory", default="")
        parser.add_argument("-d", "--data-dir", help="Set path of the data directory", default="")
        parser.add_argument("-m", "--macros-dir", help="Set path of the macros directory", default="")
        parser.add_argument("-n", "--do-not-store-results", help="Do not store results in pickle files",
                            action="store_true", default=False)

        DecisionEngineArgs = parser.parse_args(["path"])

        settings = arg_setter(DecisionEngineArgs, settings)

        self.assertDictEqual({'dataDir': self.pythonpath + '/data',
                              'esRoot': self.pythonpath,
                              'batchMode': False,
                              'interactive': False,
                              'doCodeProfiling': False,
                              'resultsDir': self.pythonpath + '/results',
                              'macro': '',
                              'version': 0,
                              'logFormat': '%(asctime)s %(levelname)s [%(module)s/%(funcName)s]: %(message)s',
                              'macrosDir': self.pythonpath + '/tutorials',
                              'logLevel': 20,
                              'doNotStoreResults': False,
                              'analysisName': '',
                              'all_mongo_collections': None,
                              'templatesDir': self.pythonpath + '/templates',
                              'seed': 0,
                              'storeResultsEachChain': False}, settings,
                             'The default settings are not set properly by arg_setter')

    def test_arg_setter(self):
        settings = ConfigObject()

        parser = argparse.ArgumentParser()
        parser.add_argument("configFile", nargs="+", help="configuration file to execute")
        parser.add_argument("-L", "--log-level", help="set log level",
                            choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL", "OFF"])
        parser.add_argument("-F", "--log-format", help="format of log messages",
                            default="%(asctime)s %(levelname)s [%(module)s/%(funcName)s]: %(message)s")
        parser.add_argument("-S", "--seed", type=int, help="set the random seed for toy generation",
                            default=settings['seed'])
        parser.add_argument("-B", "--batch-mode", help="run in batch mode, not using X Windows",
                            action="store_true", default=True)
        parser.add_argument("-i", "--interactive", help="remain in interactive mode after running",
                            action="store_true", default=True)
        parser.add_argument("-b", "--begin-with-chain", help="begin running from particular chain in chain-list",
                            default="")
        parser.add_argument("-e", "--end-with-chain", help="last chain to run in chain-list", default="")
        parser.add_argument("-s", "--single-chain", help="select which single chain to run", default="")
        parser.add_argument("-w", "--store-intermediate-result",
                            help="store intermediate result after each chain, not only at end",
                            action="store_true", default=True)
        parser.add_argument("-W", "--store-intermediate-result-one-chain",
                            help="store intermediate result of one chain",
                            default="")
        parser.add_argument("-c", "--cmd", help="python commands to process (semi-colon-seperated)")
        parser.add_argument("-U", "--userArg", help="arbitrary user argument(s)", default="")
        parser.add_argument("-P", "--run-profiling",
                            help="Run a python profiler during main Eskapade execution",
                            action="store_true")
        parser.add_argument("-v", "--data-version", help="use the samples for training containing this version number",
                            type=int, default=0)
        parser.add_argument("-a", "--analysis-name", help="The name of the analysis", default="")
        parser.add_argument("-u", "--unpickle-config", help="Unpickle configuration object from configuration file.",
                            action="store_true", default=False)
        parser.add_argument("-r", "--results-dir", help="Set path of the storage results directory", default="")
        parser.add_argument("-d", "--data-dir", help="Set path of the data directory", default="")
        parser.add_argument("-m", "--macros-dir", help="Set path of the macros directory", default="")
        parser.add_argument("-n", "--do-not-store-results", help="Do not store results in pickle files",
                            action="store_true", default=True)

        DecisionEngineArgs = parser.parse_args(["path", "-d=data", "--results-dir=result", "-v 3"])

        settings = arg_setter(DecisionEngineArgs, settings)

        self.assertDictEqual({'dataDir': 'data',
                              'esRoot': self.pythonpath,
                              'batchMode': True,
                              'interactive': True,
                              'doCodeProfiling': False,
                              'resultsDir': 'result',
                              'macro': '',
                              'version': 3,
                              'logFormat': '%(asctime)s %(levelname)s [%(module)s/%(funcName)s]: %(message)s',
                              'macrosDir': self.pythonpath + '/tutorials',
                              'logLevel': 20,
                              'doNotStoreResults': True,
                              'analysisName': '',
                              'all_mongo_collections': None,
                              'templatesDir': self.pythonpath + '/templates',
                              'seed': 0,
                              'storeResultsEachChain': True}, settings,
                             'The non-default settings are not set properly by arg_setter')

    def tearDown(self):
        pass
