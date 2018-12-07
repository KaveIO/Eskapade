import os
import shutil
import unittest
import warnings

from eskapade import ConfigObject
from eskapade import process_manager
from eskapade import resources
from escore.core import execution, definitions, persistence
from eskapade.logger import LogLevel

class IntegrationTest(unittest.TestCase):
    """Base class for Eskapade integration tests"""

    def setUp(self):
        """Set up test"""

        execution.reset_eskapade()
        settings = process_manager.service(ConfigObject)
        settings['analysisName'] = self.__class__.__name__
        settings['logLevel'] = LogLevel.DEBUG
        settings['batchMode'] = True

    def tearDown(self):
        """Tear down test"""

        # remove persisted results for this test
        path = persistence.io_dir('ana_results')
        if os.path.exists(path):
            shutil.rmtree(path)

        # reset run process
        execution.reset_eskapade()


class TutorialMacrosTest(IntegrationTest):
    """Integration tests based on tutorial macros"""

    maxDiff = None

    def eskapade_run(self, macro, return_status=definitions.StatusCode.Success):
        """Run Eskapade"""

        settings = process_manager.service(ConfigObject)
        settings['logLevel'] = LogLevel.DEBUG
        settings['macro'] = macro
        status = execution.eskapade_run(settings)
        self.assertTrue(status == return_status)


class NotebookTest(IntegrationTest):
    """Unit test notebook"""

    def run_notebook(self, notebook, analysis_name = ''):
        """ Test notebook """

        # Don't want to create an explicit dependency on jupyter
        # if nbformat and nbconvert cannot be imported, then don't run the notebook tests
        try:
            import nbformat
            from nbconvert.preprocessors import ExecutePreprocessor
            from nbconvert.preprocessors.execute import CellExecutionError
        except:
            warnings.warn('Cannot import nbformat and/or nbconvert.\nSkipping test of: {0:s}'.format(notebook))
            return

        settings = process_manager.service(ConfigObject)
        settings['notebook'] = resources.notebook(notebook)
        settings['testing'] = True
        settings['executed_notebook'] = None
        if analysis_name:
            settings['analysisName'] = analysis_name

        # load notebook
        with open(settings['notebook']) as f:
            nb = nbformat.read(f, as_version=4)

        # execute notebook
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        try:
            ep.preprocess(nb, {})
            status = True
        except CellExecutionError:
            status = False
            settings['executed_notebook'] = resources.notebook(notebook).replace('.ipynb','_executed.ipynb')
            with open(settings['executed_notebook'], mode='wt') as f:
                nbformat.write(nb, f)

        # check status
        self.assertTrue(status, 'Notebook execution failed (%s)' % settings['executed_notebook'])
