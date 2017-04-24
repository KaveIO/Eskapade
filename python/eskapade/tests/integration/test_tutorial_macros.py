import unittest
import os
import shutil

from eskapade.core import execution, definitions, persistence
from eskapade import ProcessManager, ConfigObject


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

    def tearDown(self):
        """Tear down test"""

        # remove persisted results for this test
        path = persistence.io_dir('ana_results', ProcessManager().service(ConfigObject).io_conf())
        if os.path.exists(path):
            shutil.rmtree(path)

        # reset run process
        execution.reset_eskapade()

    def run_eskapade(self, macro, return_status=definitions.StatusCode.Success):
        """Run Eskapade"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        settings['macro'] = persistence.io_path('macros', settings.io_conf(), macro)
        status = execution.run_eskapade(settings)
        self.assertTrue(status == return_status)
