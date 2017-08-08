import logging

from setuptools import setup
from setuptools import find_packages

logging.basicConfig()
logger = logging.getLogger(__file__)


def exclude_packages():
    # We don't install tests by default.
    exclude = ['*tests*']

    try:
        import ROOT
        import RooFit
        import RooStats
    except ImportError:
        logger.fatal('PyROOT and RooFit are missing! Not going to install ROOT analysis modules!')
        exclude.append('*root_analysis*')

    return exclude

setup(
    name='Eskapade',
    version='0.6-dev',
    url='http://eskapade.kave.io',
    license='',
    author='KPMG',
    description='Eskapade modular analytics',
    python_requires='>=3.5',
    package_dir={'': 'python'},
    packages=find_packages(where='python', exclude=exclude_packages()),
    install_requires=[
        'jupyter>=1.0.0',
        'matplotlib>=2.0.2',
        'numpy>=1.13.1',
        'scipy>=0.19.1',
        'statsmodels>=0.8.0',
        'pandas>=0.20.3',
        'Sphinx>=1.6.3',
        'sphinx_rtd_theme>=0.2.5b1',
        'tabulate>=0.7.7',
        'sortedcontainers>=1.5.7',
        'histogrammar>=1.0.8',
        'names>=0.3.0',
        'fastnumbers>=2.0.1',
        'python-Levenshtein>=0.12.0'
    ],
)
