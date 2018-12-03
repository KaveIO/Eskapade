from eskapade_python.bases import NotebookTest

class PipelineNotebookTest(NotebookTest):
    """Unit test notebook"""

    def test_tutorial_1(self):
        self.run_notebook('tutorial_1.ipynb', 'Tutorial_1')
