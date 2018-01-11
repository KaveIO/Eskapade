# flake8: noqa
try:
    import pyspark
except ImportError:
    from eskapade import MissingSparkError
    raise MissingSparkError()

try:
    import py4j
except ImportError:
    from eskapade import MissingPy4jError
    raise MissingPy4jError()

from eskapade.spark_analysis import decorators, data_conversion, functions
from eskapade.spark_analysis.spark_manager import SparkManager
from eskapade.spark_analysis.links import *

import eskapade.utils
eskapade.utils.set_matplotlib_backend(silent=False)
