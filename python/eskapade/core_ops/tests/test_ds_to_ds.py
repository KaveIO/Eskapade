import unittest

from eskapade.tests.observers import MockDataStoreObserver, TestCaseObservable


class DsToDsTest(unittest.TestCase, TestCaseObservable):

    def setUp(self):
        observers = [MockDataStoreObserver()]
        super(DsToDsTest, self).set_up_observers(observers)

    def test_execute(self):
        from eskapade import process_manager, DataStore
        from eskapade.core_ops.links import DsToDs

        ds = process_manager.service(DataStore)
        ds['test'] = 1
        ds_to_ds = DsToDs()
        ds_to_ds.readKey = 'test'
        ds_to_ds.storeKey = 'moved_test'
        ds_to_ds.execute()

        self.assertIn('moved_test', list(ds.keys()), 'new key not in datastore')
        self.assertNotIn('test', list(ds.keys()), 'old key still in datastore')
        self.assertEqual(ds['moved_test'], 1, 'key-value pair not consistent')

    def tearDown(self):
        super(DsToDsTest, self).tear_down_observers()
        from eskapade.core import execution
        execution.reset_eskapade()
