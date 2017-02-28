#!/usr/bin/env python

import unittest

from eskapade.core import core_tests
from eskapade.core_ops import core_ops_tests
from eskapade.analysis import analysis_tests


if __name__ == "__main__":
    test_loader = unittest.TestLoader()
    suite = test_loader.loadTestsFromModule(core_tests)
    suite.addTests(test_loader.loadTestsFromModule(core_ops_tests))
    suite.addTests(test_loader.loadTestsFromModule(analysis_tests))

    unittest.TextTestRunner(verbosity=2).run(suite)
