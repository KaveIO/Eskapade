#!/usr/bin/env python3

import unittest

import eskapade
from eskapade import eskapade_tests
from eskapade.core import core_tests
from eskapade.core_ops import core_ops_tests
from eskapade.analysis import analysis_tests
try:
    from eskapade.root_analysis import root_analysis_tests
except (eskapade.MissingRootError, eskapade.MissingRooFitError):
    print('Warning: skipping ROOT-analysis tests because ROOT could not be imported')
    root_analysis_tests = None


if __name__ == "__main__":
    test_loader = unittest.TestLoader()
    suite = test_loader.loadTestsFromModule(eskapade_tests)
    suite.addTests(test_loader.loadTestsFromModule(core_tests))
    suite.addTests(test_loader.loadTestsFromModule(core_ops_tests))
    suite.addTests(test_loader.loadTestsFromModule(analysis_tests))
    if root_analysis_tests:
        suite.addTests(test_loader.loadTestsFromModule(root_analysis_tests))

    unittest.TextTestRunner(verbosity=2).run(suite)
