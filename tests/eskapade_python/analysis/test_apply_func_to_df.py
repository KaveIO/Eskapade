import unittest

import pandas as pd

from eskapade_python.observers import MockDataStoreObserver, TestCaseObservable

''' TODO: test full functionality, i.e.:
    - applyFuncs (column and row-wise)
    - groupby
'''


class ApplyFuncToDfTest(unittest.TestCase, TestCaseObservable):

    def setUp(self):
        observers = [MockDataStoreObserver()]
        super().set_up_observers(observers)

    def test_execute(self):
        from eskapade import process_manager
        from eskapade import DataStore
        from eskapade.analysis import ApplyFuncToDf

        # --- setup a dummy data frame
        df = pd.DataFrame({'a': ['aap', 'noot', 'mies'], 'b': [0, 1, 2], 'c': [0, 1, 1], 'd': [1, 'a', None]})

        # --- setup datastore
        ds = process_manager.service(DataStore)
        ds['test_input'] = df

        # --- setup the link
        link = ApplyFuncToDf()
        link.add_columns = {'foo': 'bar'}
        link.read_key = 'test_input'
        link.store_key = 'test_output'
        link.execute()

        # --- the actual detests

        # stored at all?
        self.assertIn('test_output', ds, 'DataFrame not stored')

        # added a column?
        self.assertIn('foo', ds['test_output'].columns, 'Column not added to DataFrame')

    def tearDown(self):
        super(ApplyFuncToDfTest, self).tear_down_observers()
        from escore.core import execution
        execution.reset_eskapade()
