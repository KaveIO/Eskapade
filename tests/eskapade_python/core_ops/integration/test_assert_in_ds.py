import unittest

import pandas as pd

from eskapade_python.observers import MockDataStoreObserver, TestCaseObservable


class AssertInDsTest(unittest.TestCase, TestCaseObservable):

    def setUp(self):
        observers = [MockDataStoreObserver()]
        super(AssertInDsTest, self).set_up_observers(observers)

    def test_execute(self):
        from eskapade import process_manager
        from eskapade import DataStore
        from eskapade.core_ops.links import AssertInDs

        ds = process_manager.service(DataStore)
        ds['test1'] = pd.DataFrame([1], columns=['data'])
        ds['test2'] = pd.DataFrame([2], columns=['data'])
        ds['test3'] = pd.DataFrame([3], columns=['data'])
        aids = AssertInDs()

        aids.keySet = ['test1', 'test2', 'test3']

        aids.initialize()
        aids.execute()
        aids.finalize()

        # There is no output to test against.
        self.assertIn('test1', ds, 'dataframe not in datastore')
        self.assertIn('test2', ds, 'dataframe not in datastore')
        self.assertIn('test3', ds, 'dataframe not in datastore')

    def tearDown(self):
        super(AssertInDsTest, self).tear_down_observers()
        from eskapade.core import execution
        execution.reset_eskapade()
