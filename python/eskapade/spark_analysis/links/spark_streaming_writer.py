# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : SparkStreamingWriter                                                  *
# * Created: 2017/07/12                                                            *
# * Description:                                                                   *
# *      This link writes Spark Stream DStream data to disk. The path specifies    *
# *      the directory on eithter local disk or HDFS where files are stored.       *
# *      Each processed RDD batch will be stored in a separate file (hence the     *
# *      number of files can increase rapidly).                                    *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
from eskapade.spark_analysis import SparkManager
from pyspark.sql import Row


class SparkStreamingWriter(Link):
    """Link to write Spark Stream to disk"""

    def __init__(self, **kwargs):
        """Initialize SparkStreamingWriter instance

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str path: the directory path of the output files (local disk or HDFS)
        :param str suffix: the suffix of the file names in the output directory
        :param int repartition: repartition RDD to number of files (default: single file per batch)
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'SparkStreamingWriter'))

        # process keywords
        self._process_kwargs(kwargs, read_key=None, store_key=None, path=None, suffix=None, repartition=1)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize SparkStreamingWriter"""

        return StatusCode.Success

    def execute(self):
        """Execute SparkStreamingWriter"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        data = ds[self.read_key]

        if self.repartition:
            data = data.repartition(self.repartition)
        data.saveAsTextFiles(self.path, suffix=self.suffix)

        return StatusCode.Success

    def finalize(self):
        """Finalize SparkStreamingWriter"""

        return StatusCode.Success
