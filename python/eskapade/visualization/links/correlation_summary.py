# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : correlation_summary                                                   *
# * Created: 2017/03/13                                                            *
# * Description:                                                                   *
# *      Algorithm to do create correlation heatmaps                               *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import os
import pandas as pd
import numpy as np
#from sklearn.feature_selection import mutual_info_regression

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
from eskapade.core import persistence

#ALL_CORRS = ['pearson', 'kendall', 'spearman', 'mutual_information', 'correlation_ratio']
ALL_CORRS = ['pearson', 'kendall', 'spearman', 'correlation_ratio']
LINEAR_CORRS = ['pearson', 'kendall', 'spearman']


class CorrelationSummary(Link):
    """Create a heatmap of correlations between dataframe variables"""

    def __init__(self, **kwargs):
        """Initialize CorrelationSummary instance

        :param str name: name of link
        :param str read_key: key of input dataframe to read from data store
        :param str write_key: key of correlations dataframe in data store
        :param str results_path: path to save correlation summary pdf
        :param str method: method of computing correlations
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'correlation_summary'))

        # process arguments
        self._process_kwargs(kwargs, read_key=None, write_key=None, results_path='', method='pearson')
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize CorrelationSummary"""

        # get I/O configuration
        io_conf = ProcessManager().service(ConfigObject).io_conf()

        # get path to results directory
        if not self.results_path:
            self.results_path = persistence.io_path('results_data', io_conf, 'report')

        # check if output directory exists
        if os.path.exists(self.results_path):
            # check if path is a directory
            if not os.path.isdir(self.results_path):
                self.log().critical('output path "%s" is not a directory', self.results_path)
                raise AssertionError('output path is not a directory')
        else:
            # create directory
            self.log().debug('Making output directory "%s"', self.results_path)
            os.makedirs(self.results_path)

        # check method
        if self.method not in ALL_CORRS:
            logstring = '"{}" is not a valid correlation method, please use one of {}; using "pearson"'
            logstring = logstring.format(self.method, ', '.join(['"' + m + '"' for m in ALL_CORRS]))
            self.log().error(logstring)
            self.method = 'pearson'

        # check input arguments
        self.check_arg_types(read_key=str, method=str)
        self.check_arg_vals('read_key')

        return StatusCode.Success

    def execute(self):
        """Execute CorrelationSummary"""

        ds = ProcessManager().service(DataStore)

        import matplotlib.pyplot as plt
        from matplotlib import colors

        # fetch and check input data frame
        # drop all-nan columns right away
        df = ds.get(self.read_key, None).dropna(how='all', axis=1)
        if not isinstance(df, pd.DataFrame):
            self.log().critical('no Pandas data frame "%s" found in data store for %s', self.read_key, str(self))
            raise RuntimeError('no input data found for %s' % str(self))

        # compute correlations between all numerical variables
        self.log().debug('Computing "%s" correlations of dataframe "%s"', self.method, self.read_key)

        # mutual info, from sklearn
        if self.method == 'mutual_information':
            # numerical columns only
            cols = df.select_dtypes(include=[np.number]).columns

            # initialize correlation matrix
            n = len(cols)
            cors = np.zeros((n, n))
            for i, c in enumerate(cols):
                # compare each column to all of the columns
                cors[i, :] = mutual_info_regression(df[cols], df[c])

            cors = pd.DataFrame(cors, columns=cols, index=cols)

        elif self.method == 'correlation_ratio':
            # numerical columns only
            cols = df.select_dtypes(include=[np.number]).columns

            # choose bins for each column
            bins = {c: len(np.histogram(df[c])[1]) for c in cols}

            # sort rows into bins
            for c in cols:
                df[str(c) + '_bin'] = pd.cut(df[c], bins[c])

            # initialize correlation matrix
            n = len(cols)
            cors = np.zeros((n, n))

            for i, x in enumerate(cols):
                xbin = str(x) + '_bin'

                # definition from Wikipedia "correlation ratio"
                y_given_x = (df.groupby(xbin))[cols]
                weighted_var_y_bar = (y_given_x.count() * (y_given_x.mean() - df.mean()) ** 2).sum()
                weighted_var_y = df[cols].count() * df[cols].var()

                cors[i, :] = weighted_var_y_bar / weighted_var_y

            cors = pd.DataFrame(cors, columns=cols, index=cols)

        else:
            cors = df.corr(method=self.method)
            cols = list(cors.columns)

        # set up heatmap of convenient size
        plot_size = max(len(cols) / 1.8, 2)
        fig, ax = plt.subplots(figsize=(1.5 * plot_size, plot_size))

        vmin = -1 if self.method in LINEAR_CORRS else 0
        vmax = 1
        cmap = 'RdYlGn' if self.method in LINEAR_CORRS else 'YlGn'

        norm = colors.Normalize(vmin=vmin, vmax=vmax)
        img = ax.pcolormesh(cors, cmap=cmap, edgecolor='w', linewidth=1, norm=norm)

        # make plot look pretty
        ax.set_title('{0:s} correlations'.format(self.method.capitalize()))
        ax.set_yticks(np.arange(len(cols)) + 0.5)
        ax.set_xticks(np.arange(len(cols)) + 0.5)
        ax.set_yticklabels(cols, rotation='horizontal')
        ax.set_xticklabels(cols, rotation='vertical')
        fig.colorbar(img)

        # annotate with correlation values
        for i in range(len(cols)):
            for j in range(len(cols)):
                point = float(cors[cols[i]][j])
                label = 'NaN' if np.isnan(point) else '{0:.2f}'.format(point)
                white_cond = (point < 0.7 * vmin) or (point >= 0.7 * vmax) or np.isnan(point)
                color = 'w' if white_cond else 'k'
                ax.annotate(label, xy=(i + 0.5, j + 0.5), color=color, horizontalalignment='center',
                            verticalalignment='center')

        # save plots in file
        fname = '_'.join(['correlations', self.read_key.replace(' ', ''), self.method]) + '.pdf'
        fpath = os.path.join(self.results_path, fname)
        self.log().debug('Saving correlation heatmap as {}'.format(fpath))
        fig.savefig(fpath, bbox_inches='tight')

        # save correlations to datastore if requested
        if self.write_key:
            ds[self.write_key] = cors

        return StatusCode.Success
