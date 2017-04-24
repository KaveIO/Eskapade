import copy
from abc import ABCMeta, abstractmethod

from eskapade import core


class TestCaseObservable(object):

    def set_up_observers(self, observers):
        self.observers = observers
        for observer in self.observers:
            observer.set_up()

    def tear_down_observers(self):
        for observer in self.observers:
            observer.tear_down()


class TestCaseObserver(object, metaclass=ABCMeta):
    @abstractmethod
    def set_up(self):
        pass

    @abstractmethod
    def tear_down(self):
        pass


class DataStoreMock(dict):

    _instances = {}

    def __new__(class_, *args, **kwargs):
        if class_ not in class_._instances:
            class_._instances[class_] = super(DataStoreMock, class_).__new__(class_, *args, **kwargs)
        return class_._instances[class_]


class MockDataStoreObserver(TestCaseObserver):

    def set_up(self):
        """ mock DataStore """
        self.old_store = copy.deepcopy(core.process_services.DataStore)
        core.process_services.DataStore = DataStoreMock


    def tear_down(self):
        """ unmock DataStore """
        core.process_services.DataStore = copy.deepcopy(self.old_store)
        del self.old_store


# class MockConfigObjectObserver(TestCaseObserver):
#
#     def set_up(self):
#         """ mock ConfigObject """
#         self.old_store = copy.deepcopy(eskapade.ConfigObject)
#         eskapade.ConfigObject =
#
#
#     def tear_down(self):
#         """ unmock ConfigObject """
#         eskapade.ConfigObject = copy.deepcopy(self.old_store)
#         del self.old_store


class MockProcessManagerObserver(TestCaseObserver):

    def set_up(self):
        """ mock process manager """
        pass

    def unmock_processManager(self):
        """ unmock process manager """
        pass


class MockMongoObserver(TestCaseObserver):

    def set_up(self):
        """ create test database """
        pass

    def tear_down(self):
        """ delete test database """
        pass
