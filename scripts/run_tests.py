#!/usr/bin/env python3

import unittest
import importlib
import argparse

import eskapade

TEST_PACKAGES = ('', 'core', 'core_ops', 'visualization', 'analysis', 'root_analysis', 'data_quality')
TEST_MODS = dict(unit='', integration='integration')
MISSING_PACKAGE_ERRORS = (eskapade.MissingRootError, eskapade.MissingRooFitError, eskapade.MissingSparkError,
                          eskapade.MissingPy4jError)

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('type', nargs='?', choices=('unit', 'integration'), default='unit',
                        help='type of test (default "unit")')
    args = parser.parse_args()
    print('Running {} tests\n'.format(args.type))

    # create test suite
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    # import test modules
    missing_exceptions = []
    for package in TEST_PACKAGES:
        mod_name = '.'.join(p for p in ('eskapade', package, 'tests', TEST_MODS[args.type]) if p)
        try:
            mod = importlib.import_module(mod_name)
            tests = loader.loadTestsFromModule(mod)
            suite.addTests(tests)
        except MISSING_PACKAGE_ERRORS as exc:
            missing_exceptions.append((package, exc))
        except ImportError as exc:
            if exc.name not in (mod_name, '.'.join(mod_name.split('.')[:-1])):
                raise

    # run tests
    unittest.TextTestRunner(verbosity=2).run(suite)

    # print warnings for missing packages
    for failed in missing_exceptions:
        print('Warning: did not run tests for subpackage "{0:s}" ({1:s})'.format(failed[0], str(failed[1])))
