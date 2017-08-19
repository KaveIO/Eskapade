# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : SparkHister                                                           *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to do...(fill in here)                                          *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import numpy as np

from eskapade import ProcessManager, StatusCode, DataStore, Link


class SparkHister(Link):
    """
    Defines the content of link SparkHister
    """

    def __init__(self, name='HiveHister'):
        """
        Store the configuration of link SparkHister

        :param str name: name of link
        :param str readKey: key of data to read from data store
        :param str storeKey: key of data to store in data store
        :param list columns: columns of the spark dataframe to make a histogram from
        :param dict bins: the bin edges of the histogram
        :param bool convert_for_mongo: if True the data structure of the result is converted so it can be stored in
            mongo
        """

        Link.__init__(self, name)

        self.readKey = None
        self.columns = None
        self.storeKey = None
        self.bins = {}
        self.convert_for_mongo = False
        self.save_as_csv_style = False
        self.save_as_json_style = True

        return

    def initialize(self):
        """ Initialize SparkHister """

        return StatusCode.Success

    def execute(self):
        """ Execute SparkHister """
        ds = ProcessManager().service(DataStore)

        spark_df = ds[self.readKey]

        result = {}

        BINS = self.bins
        for c in self.columns:
            pos = spark_df.columns.index(c)
            self.log().debug("Processing column: %s", c)
            if c in list(self.bins.keys()):
                result[c] = spark_df.map(lambda r: r[pos]).histogram(BINS[c])
            else:
                result[c] = spark_df.map(lambda r: r[pos]).histogram(25)

        # --- NOTE: this depends on how the data will be read by the BI tool
        #           - SQL/Tableau requires a row per histogram bin
        #           - QlikSense supports a more JSON-like style by putting an entire histogram in single row
        if self.convert_for_mongo:
            r = []
            for c in self.columns:
                hist = result[c]

                if self.save_as_csv_style:
                    try:
                        binedges   = np.asarray(hist[0])
                        bincenters = 0.5*(binedges[1:] + binedges[:-1])
                        binvalues  = hist[1]
                    except:
                        self.log().error("could not extract values from histogram data")

                    # --- row per bin
                    for i in range(len(binvalues)):
                        r.append({'column': c, 'bin': bincenters[i], 'value': binvalues[i]})

                # --- alternatively row per histogram
                if self.save_as_json_style:
                    r.append({'column': c, 'edges': hist[0], 'values': hist[1]})

            result = r

        ds[self.storeKey] = result

        return StatusCode.Success

    def finalize(self):
        """ Finalizing SparkHister """

        return StatusCode.Success

