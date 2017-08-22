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

import eskapade.utils
eskapade.utils.set_matplotlib_backend(silent=False)
from . import decorators, data_conversion, functions
from .spark_manager import SparkManager
from .links import *
