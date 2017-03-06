#!/usr/bin/env python3

import unittest

from eskapade.eskapade_tests import test_tutorial_macros


if __name__ == "__main__":
    test_loader = unittest.TestLoader()
    suite = test_loader.loadTestsFromModule(test_tutorial_macros)

    unittest.TextTestRunner(verbosity=2).run(suite)
