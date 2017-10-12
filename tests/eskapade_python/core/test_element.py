import unittest

from eskapade.core import execution
from eskapade.core.definitions import StatusCode
from eskapade.core.element import Chain
from eskapade.core.meta import Processor


class ChainTest(unittest.TestCase):
    def setUp(self):
        class DummyChain(Chain):
            def __init__(self, name):
                super().__init__(name)
                self.init = []
                self.exec = []
                self.fin = []

            def clear(self):
                super().clear()
                self.init.clear()
                self.exec.clear()
                self.fin.clear()

        class DummyLink(Processor):
            def initialize(self):
                self.parent.init.append(self.name + 'Init')
                return StatusCode.Success

            def execute(self):
                self.parent.exec.append(self.name + 'Exec')
                return StatusCode.Success

            def finalize(self):
                self.parent.fin.append(self.name + 'Fin')
                return StatusCode.Success

        class SkipLink(DummyLink):
            def __init__(self):
                super().__init__('SkipLink')

            def initialize(self):
                self.parent.init.append(self.name + 'InitSkip')
                return StatusCode.SkipChain

            def execute(self):
                self.parent.exec.append(self.name + 'ExecSkip')
                return StatusCode.SkipChain

        class RepeatLink(DummyLink):
            def __init__(self):
                super().__init__('RepeatLink')

            def execute(self):
                self.parent.exec.append(self.name + 'ExecRepeat')
                return StatusCode.RepeatChain

        class FailLink(DummyLink):
            def __init__(self):
                super().__init__('FailLink')

            def initialize(self):
                self.parent.init.append(self.name + 'InitFail')
                return StatusCode.Failure

            def execute(self):
                self.parent.exec.append(self.name + 'ExecFail')
                return StatusCode.Failure

            def finalize(self):
                self.parent.fin.append(self.name + 'FinFail')
                return StatusCode.Failure

        class UnhandledLink(DummyLink):
            def __init__(self):
                super().__init__('UnhandledLink')

            def initialize(self):
                self.parent.init.append(self.name + 'InitUnhandled')
                return StatusCode.Undefined

            def execute(self):
                self.parent.exec.append(self.name + 'ExecUnhandled')
                return StatusCode.Undefined

            def finalize(self):
                self.parent.fin.append(self.name + 'FinUnhandled')
                return StatusCode.Undefined

        self.link_one = DummyLink('LinkOne')
        self.link_two = DummyLink('LinkTwo')
        self.link_three = DummyLink('LinkThree')
        self.link_four = DummyLink('LinkFour')

        self.links = [self.link_one, self.link_two, self.link_three, self.link_four]

        self.skip_link = SkipLink()
        self.repeat_link = RepeatLink()
        self.fail_link = FailLink()
        self.unhandled_link = UnhandledLink()

        self.dummy_chain = DummyChain('DummyChain')
        [self.dummy_chain.add(_) for _ in self.links]

    def tearDown(self):
        execution.reset_eskapade()

    def test_adding_links(self):
        chain = Chain('Chain')
        [chain.add(_) for _ in self.links]

        # Assert that forward/insertion order is maintained.
        self.assertEqual(list(chain),
                         self.links,
                         msg='Links order mismatch!')

        # Assert that the reversed order is maintained.
        self.assertEqual(list(reversed(chain)),
                         list(reversed(self.links)),
                         msg='Links reverse order mismatch!')

    def test_pop_front_link(self):
        chain = Chain('Chain')
        [chain.add(_) for _ in self.links]

        # Assert the front is popped.
        self.assertEqual(chain.pop(last=False),
                         self.links.pop(0),
                         msg='Popped linked is not first link!')

        # Assert that the order and number of elements are maintained.
        self.assertEqual(list(chain),
                         self.links,
                         msg='Links order and number mismatch!')

    def test_pop_last_link(self):
        chain = Chain('Chain')
        [chain.add(_) for _ in self.links]

        self.assertEqual(chain.pop(last=True),
                         self.links.pop(),
                         msg='Popped linked is not last link!')

        # Assert that the order and number of elements are maintained.
        self.assertEqual(list(chain),
                         self.links,
                         msg='Links order and number mismatch!')

    def test_initialize(self):
        # Test Success.
        status = self.dummy_chain.initialize()

        self.assertEqual(status,
                         StatusCode.Success,
                         msg='Initialization failed!')

        self.assertEqual(self.dummy_chain.init,
                         [_.name + 'Init' for _ in self.links],
                         msg='Initialization order and number mismatch!')

        # Test Fail.
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.fail_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.initialize()

        self.assertEqual(status,
                         StatusCode.Failure,
                         msg='Initialization did not fail!')

        expected = [_.name + 'Init' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Fail'

        self.assertEqual(self.dummy_chain.init,
                         expected,
                         msg='Initialization order and number mismatch!')

        # Test skip
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.skip_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.initialize()

        self.assertEqual(status,
                         StatusCode.SkipChain,
                         msg='Initialization did not skip!')

        expected = [_.name + 'Init' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Skip'

        self.assertEqual(self.dummy_chain.init,
                         expected,
                         msg='Initialization order and number mismatch!')

        # Test Unhandled
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.unhandled_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.initialize()

        self.assertEqual(status,
                         StatusCode.Undefined,
                         msg='Initialization did not handle!')

        expected = [_.name + 'Init' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Unhandled'

        self.assertEqual(self.dummy_chain.init,
                         expected,
                         msg='Initialization order and number mismatch!')

    def test_execute(self):
        status = self.dummy_chain.execute()

        self.assertEqual(status,
                         StatusCode.Success,
                         msg='Execution failed!')

        self.assertEqual(self.dummy_chain.exec,
                         [_.name + 'Exec' for _ in self.links],
                         msg='Execution order and number mismatch!')

        # Test Fail.
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.fail_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.execute()

        self.assertEqual(status,
                         StatusCode.Failure,
                         msg='Execution did not fail!')

        expected = [_.name + 'Exec' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Fail'

        self.assertEqual(self.dummy_chain.exec,
                         expected,
                         msg='Execution order and number mismatch!')

        # Test skip
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.skip_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.execute()

        self.assertEqual(status,
                         StatusCode.SkipChain,
                         msg='Execution did not skip!')

        expected = [_.name + 'Exec' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Skip'

        self.assertEqual(self.dummy_chain.exec,
                         expected,
                         msg='Execution order and number mismatch!')

        # Test repeat
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.repeat_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.execute()

        self.assertEqual(status,
                         StatusCode.RepeatChain,
                         msg='Execution did not repeat!')

        expected = [_.name + 'Exec' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Repeat'

        self.assertEqual(self.dummy_chain.exec,
                         expected,
                         msg='Execution order and number mismatch!')

        # Test Unhandled
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.unhandled_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.execute()

        self.assertEqual(status,
                         StatusCode.Undefined,
                         msg='Execution did handle!')

        expected = [_.name + 'Exec' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Unhandled'

        self.assertEqual(self.dummy_chain.exec,
                         expected,
                         msg='Execution order and number mismatch!')

    def test_finalize(self):
        status = self.dummy_chain.finalize()

        self.assertEqual(status,
                         StatusCode.Success,
                         msg='Finalization failed!')

        self.assertEqual(self.dummy_chain.fin,
                         [_.name + 'Fin' for _ in self.links],
                         msg='Finalization order and number mismatch!')

        # Test Fail.
        self.dummy_chain.clear()

        links = [self.link_one, self.link_two, self.fail_link, self.link_three, self.link_four]
        [self.dummy_chain.add(_) for _ in links]

        status = self.dummy_chain.finalize()

        self.assertEqual(status,
                         StatusCode.Failure,
                         msg='Finalization did not fail!')

        expected = [_.name + 'Fin' for _ in links[:3]]
        expected[-1] = expected[-1] + 'Fail'

        self.assertEqual(self.dummy_chain.fin,
                         expected,
                         msg='Finalization order and number mismatch!')
