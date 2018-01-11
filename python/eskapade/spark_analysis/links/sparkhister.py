"""Project: Eskapade - A python-based package for data analysis.

Class: SparkHister

Created: 2016/11/08

Description:
    Algorithm to do...(fill in here)

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np

from eskapade import process_manager, StatusCode, DataStore, Link


class SparkHister(Link):
    """Defines the content of link SparkHister."""

    # TODO: Fix class docstring.

    def __init__(self, name='HiveHister'):
        """Initialize link instance.

        Store the configuration of link SparkHister.

        :param str name: name of link
        :param str read_key: key of data to read from data store
        :param str store_key: key of data to store in data store
        :param list columns: columns of the Spark dataframe to make a histogram from
        :param dict bins: the bin edges of the histogram
        :param bool convert_for_mongo: if True the data structure of the result is converted so it can be stored in
            mongo
        """
        Link.__init__(self, name)

        self.read_key = None
        self.columns = None
        self.store_key = None
        self.bins = {}
        self.convert_for_mongo = False
        self.save_as_csv_style = False
        self.save_as_json_style = True

    def initialize(self):
        """Initialize the link."""
        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        spark_df = ds[self.read_key]

        result = {}

        for c in self.columns:
            pos = spark_df.columns.index(c)
            self.logger.debug("Processing column: {col}.", col=c)
            result[c] = spark_df.map(lambda r: r[pos]).histogram(self.bins.get(c, 25))

        # --- NOTE: this depends on how the data will be read by the BI tool
        #           - SQL/Tableau requires a row per histogram bin
        #           - QlikSense supports a more JSON-like style by putting an entire histogram in single row
        if self.convert_for_mongo:
            r = []
            for c in self.columns:
                hist = result[c]

                if self.save_as_csv_style:
                    try:
                        binedges = np.asarray(hist[0])
                        bincenters = 0.5 * (binedges[1:] + binedges[:-1])
                        binvalues = hist[1]
                    except Exception:
                        self.logger.error("Could not extract values from histogram data.")

                    # --- row per bin
                    for i in range(len(binvalues)):
                        r.append({'column': c, 'bin': bincenters[i], 'value': binvalues[i]})

                # --- alternatively row per histogram
                if self.save_as_json_style:
                    r.append({'column': c, 'edges': hist[0], 'values': hist[1]})

            result = r

        ds[self.store_key] = result

        return StatusCode.Success

    def finalize(self):
        """Finalize the link."""
        return StatusCode.Success
