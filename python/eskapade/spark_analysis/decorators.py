"""Project: Eskapade - A python-based package for data analysis.

Module: spark_analysis.decorators

Created: 2017/05/24

Description:
    Decorators for Spark objects

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pyspark
import pyspark.streaming

CUSTOM_REDUCE_CLASSES = (pyspark.rdd.RDD, pyspark.sql.DataFrame, pyspark.sql.Column,
                         pyspark.streaming.dstream.DStream,
                         pyspark.streaming.dstream.TransformedDStream)


def spark_cls_reduce(self):
    """Reduce function for Spark classes.

    Spark objects connected to distributed data cannot be stored in Pickle
    files.  This custom reduce function enables Pickling of a string
    representation of the Spark object.
    """
    return str, (str(self),)


for cls in CUSTOM_REDUCE_CLASSES:
    cls.__reduce__ = spark_cls_reduce

pyspark.sql.Column.__str__ = lambda self: self._jc.toString()
pyspark.sql.Column.__format__ = lambda self, spec: format(str(self), spec)
pyspark.sql.DataFrame.__contains__ = lambda self, key: str(key) in self.columns
