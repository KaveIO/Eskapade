# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Class  : SparkDataToCsv                                                      *
# * Created: 2015-11-16                                                          *
# *                                                                              *
# * Description:                                                                 *
# *     Write Spark data to local CSV files                                      *
# *                                                                              *
# * Authors:                                                                     *
# *      KPMG Big Data team, Amstelveen, The Netherlands                         *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import os
import shutil

import pyspark

from eskapade import Link, StatusCode, ProcessManager, DataStore, ConfigObject
from eskapade.core import persistence


class SparkDataToCsv(Link):
    """Write Spark data to local CSV files

    Data to write to CSV are provided as a Spark RDD or a Spark data frame.
    The data are written to a configurable number of CSV files in the
    specified output directory.
    """

    def __init__(self, **kwargs):
        """Initialize link instance

        :param str name: name of link instance
        :param str read_key: data-store key of the Spark data
        :param str output_path: directory path of the output CSV file(s)
        :param str mode: write mode if data already exist ("overwrite", "ignore", "error")
        :param str compression_codec: compression-codec class (e.g., 'org.apache.hadoop.io.compress.GzipCodec')
        :param str sep: CSV separator string
        :param tuple|bool header: column names to write as CSV header
                                  or boolean to indicate if names must be determined from input data frame
        :param int num_files: requested number of output files
        """

        Link.__init__(self, kwargs.pop('name', 'SparkDataToCsv'))
        self._process_kwargs(kwargs, read_key=None, output_path=None, mode='error', compression_codec=None,
                             sep=',', header=False, num_files=1)

    def initialize(self):
        """Initialize SparkDataToCsv"""

        # check input arguments
        self.check_arg_types(allow_none=True, read_key=str, output_path=str, compression_codec=str)
        self.check_arg_types(mode=str, sep=str, num_files=int)
        self.check_arg_types(recurse=True, allow_none=True)
        self.check_arg_vals('read_key', 'sep')
        self.check_arg_vals('output_path', 'compression_codec', allow_none=True)
        self.check_arg_opts(mode=('overwrite', 'ignore', 'error'))
        if self.num_files < 1:
            raise RuntimeError('requested number of files is less than 1 ({:d})'.format(self.num_files))

        # set other attributes
        self.do_execution = True

        # set default output path
        if not self.output_path:
            settings = ProcessManager().service(ConfigObject)
            self.output_path = persistence.io_path('results_data', settings.io_conf(), '{}_output'.format(self.name))

        # parse header argument
        try:
            self.header = tuple(self.header)
        except TypeError:
            self.header = bool(self.header)
        if isinstance(self.header, tuple) and not self.header:
            raise RuntimeError('empty header sequence specified')

        # check output directory
        self.output_path = os.path.abspath(self.output_path)
        if os.path.exists(self.output_path):
            # output data already exist
            if self.mode == 'ignore':
                # do not execute link
                self.log().debug('Output data already exist; not executing link')
                self.do_execution = False
                return StatusCode.Success
            elif self.mode == 'error':
                # raise exception
                raise RuntimeError('output data already exist')

            # remove output directory
            if not os.path.isdir(self.output_path):
                raise RuntimeError('output path "{}" is not a directory'.format(self.output_path))
            shutil.rmtree(self.output_path)
        elif not os.path.exists(os.path.dirname(self.output_path)):
            # create path up to the last component
            self.log().debug('Creating output path "%s"', self.output_path)
            os.makedirs(os.path.dirname(self.output_path))

        return StatusCode.Success

    def execute(self):
        """Execute SparkDataToCsv"""

        # do not execute if "do_execution" flag is not set
        if not self.do_execution:
            self.log().debug('"do_execution" flag not set; skipping execution of link')
            return StatusCode.Success

        # fetch data from data store
        ds = ProcessManager().service(DataStore)
        if self.read_key not in ds:
            raise KeyError('no data with key "{}" in data store'.format(self.read_key))
        data = ds[self.read_key]
        if not isinstance(data, (pyspark.rdd.RDD, pyspark.sql.DataFrame)):
            raise TypeError('got data of type "{}"; expected a Spark RDD/DataFrame'.format(str(type(data))))

        # convert row to string
        data = data.map(lambda r: self.sep.join(map(str, r)))

        # set number of partitions/output files
        data = data.coalesce(self.num_files, shuffle=self.num_files > data.getNumPartitions())

        # add header rows
        if self.header:
            header_str = self.sep.join(self.header)
            data = data.mapPartitions(lambda p: [header_str] + list(p), preservesPartitioning=True)

        # write data to CSV file
        data.saveAsTextFile(self.output_path, compressionCodecClass=self.compression_codec)

        return StatusCode.Success
