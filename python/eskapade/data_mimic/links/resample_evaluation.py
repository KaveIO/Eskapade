"""Project: Eskapade - A python-based package for data analysis.

Class: ResampleEvaluation

Created: 2018-07-18

Description:
    Algorithm to evaluate the statistical simularity between 2 data sets using a chiˆ2 test.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
import numpy as np
import pandas as pd
import scipy

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.data_mimic import data_mimic_util as util
from eskapade.analysis import correlation

from scipy.spatial.distance import cosine


class ResampleEvaluation(Link):
    """
    Evaluates the statistical simularity between 2 (multi-dimensional) data sets using a chiˆ2 test. The 2 data sets
    are binned and the chi^2 statistic is calculated using the counts per bin.
    """

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str data_all_read_key: key of input data to read from data store
        :param str resample_read_key: key of resampled data to read from data store
        :param sequence or int bins: Specification:
                                     * A sequence of arrays describing the bin edges along each dimension
                                     * The number of bins for each dimension (nx, ny, ... =bins)
                                     * The number of bins for all dimensions (nx=ny=...=bins)
        :param str chi2_store_key: key of chiˆ2 value to store in data store
        :param str p_value_store_key: key of p-value to store in data store
        :param str new_column_order_read_key: key of the column order of the data as saved in the datastore
        :param str ks_store_key: key to save the KS-metric in the data store
        :param str chis_store_key: key to save the chi-square calculations in the data store
        :param str distance_store_key: key to save the cosine distance calculations in the data store
        :param str df_resample_read_key: key of the saved resampled data in the data store
        :param str corr_store_key: key to save the correlations in the data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ResampleEvaluation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs,
                             data_read_key=None,
                             resample_read_key=None,
                             bins=None,
                             chi2_store_key=None,
                             p_value_store_key=None,
                             new_column_order_read_key=None,
                             ks_store_key=None,
                             chis_store_key=None,
                             distance_store_key=None,
                             df_resample_read_key=None,
                             corr_store_key=None)

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)
        # Turn off the line above, and on the line below if you wish to keep these extra kwargs.
        # self._process_kwargs(kwargs)

    def initialize(self):
        """Initialize the link.

        :returns: status code of initialization
        :rtype: StatusCode
        """
        return StatusCode.Success

    def execute(self):
        """Execute the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        ds = process_manager.service(DataStore)

        data = ds[self.data_read_key].copy()
        resample = ds[self.resample_read_key].copy()

        resample_binned = np.histogramdd(resample, bins=self.bins)
        data_binned = np.histogramdd(data, bins=self.bins)

        # -- with higher dimensions, binning will most likely result in empty bins, so we need to exclude them.
        # -- This happens especially when we have correlated data. See docs.

        # define the dof for each variable = bins for the 1d hist and 2d hists
        # will be nr. of catagories if catagorical, otherwise length of the bins that were defined for the
        # dd histogram
        dofs = [len(np.unique(data[:, x])) if x not in ds['continuous_i'] else
                len(self.bins[x]) for x in range(data.shape[1])]

        # --  Chi2 per param:
        chis = {}
        kss = {}
        for i, param in enumerate(ds[self.new_column_order_read_key]):
            data_o = data[:, i][~np.isnan(data[:, i])]
            data_r = resample[:, i][~np.isnan(resample[:, i])]

            orig = np.histogram(data_o, bins=dofs[i])
            resa = np.histogram(data_r, bins=dofs[i])

            chi2, p_value = util.scaled_chi(resa[0][orig[0] > 0],
                                            orig[0][orig[0] > 0])
            # -- savind bins as len histogram, in case 0's are removed and we're left with less bins
            chis[param] = {param: {'chi': chi2, 'p-value': p_value, 'bins': len(resa[0][orig[0] > 0])}}
            ks, p_value = scipy.stats.ks_2samp(ds[self.data_read_key][:, i],
                                               ds[self.resample_read_key][:, i])
            kss[param] = {'ks': ks, 'p-value': p_value}

        # -- first order correlations:
        for i, param1 in enumerate(ds[self.new_column_order_read_key]):
            for j, param2 in enumerate(ds[self.new_column_order_read_key]):
                if param1 != param2:
                    orig = np.histogram2d(ds[self.data_read_key][:, i], ds[self.data_read_key][:, j],
                                          bins=[self.bins[i], self.bins[j]])
                    resa = np.histogram2d(ds[self.resample_read_key][:, i], ds[self.resample_read_key][:, j],
                                          bins=[self.bins[i], self.bins[j]])

                    chi2, p_value = util.scaled_chi(resa[0][orig[0] > 0],
                                                    orig[0][orig[0] > 0])

                    chis[param1][param2] = {'chi': chi2, 'p-value': p_value, 'bins': len(self.bins[i])}

        chi2, p_value = util.scaled_chi(resample_binned[0][data_binned[0] > 0].flatten(),
                                        data_binned[0][data_binned[0] > 0].flatten())

        chis['total'] = {'total': {'chi': chi2, 'p-value': p_value}}

        distance = []
        indices = ds[self.df_resample_read_key]['ID'].values
        o = pd.DataFrame(ds[self.data_read_key][indices, :])
        o = o.values / o.fillna(0).max(0)[None, :]
        r = pd.DataFrame(ds[self.resample_read_key])
        r = r.values / r.fillna(0).max(0)[None, :]
        for n in range(ds[self.data_read_key].shape[0]):
            # when using pca, number of nans in continuous columns in resample can be different then number of nans
            # in continuous columns in original data set
            if len(o[n, ~np.isnan(o[n, :])]) == len(r[n, ~np.isnan(r[n, :])]):
                distance.append(cosine(o[n, ~np.isnan(o[n, :])], r[n, ~np.isnan(r[n, :])]))
        distance = np.array(distance)
        dis = pd.Series(distance).describe()
        dis = dis.append(pd.Series(distance.sum(), index=['sum']))

        correlations = []
        correlations.append(correlation.calculate_correlations(pd.DataFrame(data), method='pearson'))
        correlations.append(correlation.calculate_correlations(pd.DataFrame(resample), method='pearson'))

        self.logger.info('CHI2: {}'.format(chi2))
        self.logger.info('P value: {}'.format(p_value))

        ds[self.chi2_store_key] = chi2
        ds[self.p_value_store_key] = p_value

        ds[self.chis_store_key] = chis
        ds[self.ks_store_key] = kss

        ds[self.distance_store_key] = dis
        ds[self.corr_store_key] = correlations

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
