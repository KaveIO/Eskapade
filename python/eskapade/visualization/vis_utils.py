"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/02/28

Description:
    Utility functions to collect Eskapade python modules
    e.g. functions to get correct Eskapade file paths and env variables

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
import pandas as pd

from eskapade.logger import Logger

NUM_NS_DAY = 24 * 3600 * int(1e9)

logger = Logger()


def plot_histogram(hist, x_label, y_label=None, is_num=True, is_ts=False, pdf_file_name='', top=20):
    """Create and plot histogram of column values.

    :param hist: input numpy histogram = values, bin_edges
    :param str x_label: Label for histogram x-axis
    :param str y_label: Label for histogram y-axis
    :param bool is_num: True if observable to plot is numeric
    :param bool is_ts: True if observable to plot is a timestamp
    :param str pdf_file_name: if set, will store the plot in a pdf file
    :param int top: only print the top 20 characters of x-labels and y-labels. (default is 20)
    """
    # import matplotlib here to prevent import before setting backend in
    # core.execution.eskapade_run
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    plt.figure(figsize=(7, 5))

    try:
        hist_values = hist[0]
        hist_bins = hist[1]
    except BaseException:
        raise ValueError('Cannot extract binning and values from input histogram')

    assert hist_values is not None and len(
        hist_values), 'Histogram bin values have not been set.'
    assert hist_bins is not None and len(
        hist_bins), 'Histogram binning has not been set.'

    # basic attribute check: time stamps treated as numeric.
    if is_ts:
        is_num = True

    # plot numeric and time stamps
    if is_num:
        bin_edges = hist_bins
        bin_values = hist_values
        assert len(bin_edges) == len(bin_values) + 1, \
            'bin edges (+ upper edge) and bin values have inconsistent lengths: {:d} vs {:d}.'\
            .format(len(bin_edges), len(bin_values))

        if is_ts:
            # difference in seconds
            be_tsv = [pd.Timestamp(ts).value for ts in bin_edges]
            width = np.diff(be_tsv)
            # pd.Timestamp(ts).value is in ns
            # maplotlib dates have base of 1 day
            width = width / NUM_NS_DAY
        else:
            width = np.diff(bin_edges)

        # plot histogram
        plt.bar(bin_edges[:-1], bin_values, width=width)

        # set x-axis properties
        plt.xlim(min(bin_edges), max(bin_edges))
        plt.xticks(fontsize=12, rotation=90 if is_ts else 0)
    # plot categories
    else:
        labels = hist_bins
        values = hist_values
        assert len(labels) == len(values), \
            'labels and values have different array lengths: {:d} vs {:d}.'.format(len(labels), len(values))

        # plot histogram
        tick_pos = np.arange(len(labels)) + 0.5
        plt.bar(tick_pos - 0.4, values, width=0.8)

        # set x-axis properties
        def xtick(lab):
            """Get x-tick."""
            lab = str(lab)
            if len(lab) > top:
                lab = lab[:17] + '...'
            return lab

        plt.xlim((0., float(len(labels))))
        plt.xticks(tick_pos, [xtick(lab)
                              for lab in labels], fontsize=12, rotation=90)

    # set common histogram properties
    plt.xlabel(x_label, fontsize=14)
    plt.ylabel(
        str(y_label) if y_label is not None else 'Bin count',
        fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid()

    # store plot
    if pdf_file_name:
        pdf_file = PdfPages(pdf_file_name)
        plt.savefig(pdf_file, format='pdf', bbox_inches='tight', pad_inches=0)
        plt.close()
        pdf_file.close()


def plot_2d_histogram(hist, x_lim, y_lim, title, x_label, y_label, pdf_file_name):
    """Plot 2d histogram with matplotlib.

    :param hist: input numpy histogram = x_bin_edges, y_bin_edges, bin_entries_2dgrid
    :param tuple x_lim: range tuple of x-axis (min,max)
    :param tuple y_lim: range tuple of y-axis (min,max)
    :param str title: title of plot
    :param str x_label: Label for histogram x-axis
    :param str y_label: Label for histogram y-axis
    :param str pdf_file_name: if set, will store the plot in a pdf file
    """
    # import matplotlib here to prevent import before setting backend in
    # core.execution.eskapade_run
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    plt.figure(figsize=(7, 5))

    try:
        x_ranges = hist[0]
        y_ranges = hist[1]
        grid = hist[2]
    except BaseException:
        raise ValueError('Cannot extract ranges and grid from input histogram')

    ax = plt.gca()
    ax.pcolormesh(x_ranges, y_ranges, grid)
    ax.set_ylim(y_lim)
    ax.set_xlim(x_lim)
    ax.set_title(title)

    plt.xlabel(x_label, fontsize=14)
    plt.ylabel(y_label, fontsize=14)
    plt.grid()

    if pdf_file_name:
        pdf_file = PdfPages(pdf_file_name)
        plt.savefig(pdf_file, format='pdf', bbox_inches='tight', pad_inches=0)
        plt.close()
        pdf_file.close()


def delete_smallstat(df, group_col, statlim=400):
    """Remove low-statistics groups from dataframe.

    Function to make a new DataFrame that removes all groups of group_col that have less than statlim entries.

    :param df: pandas DataFrame
    :param str group_col: name of the column to group on
    :param int statlim: number of entries a group has to have to be statistically significant
    :returns: smaller DataFrame and the number of removed categories
    :rtype: tuple
    """
    sizes = df.groupby(group_col).size()
    group_sizes = list(zip(df.groupby(group_col).size().index, sizes))

    # Check if there are groups that are too small, if not, return the original DataFrame
    number_toosmall = (sizes < statlim).sum()
    if number_toosmall == 0:
        return df, 0

    # Define the selections for the groups that are too small
    i = 0
    for group, lim in group_sizes:
        if lim < statlim:
            if i == 0:
                selstring = (df[group_col] != group)
            else:
                # Kept this line explicit for clarity
                selstring = selstring & (df[group_col] != group)
            i += 1

    # Return the smaller DataFrame
    df_small = df[selstring]
    return df_small, i


def box_plot(
        df, cause_col, result_col='cost', pdf_file_name='', ylim_quant=0.95, ylim_high=None, ylim_low=0, rot=90,
        statlim=400, label_dict=None, title_add='', top=20):
    """Make box plot.

    Function that plots the boxplot of the column df[result_col] in groups of cause_col. This means that
    the DataFrame is grouped-by on the cause column and then the distribution per group is plotted in a boxplot
    using the standard pandas functionality.
    Boxplots with less than statlim (default=400 ) entries in it are automatically removed.

    :param df: pandas DataFrame
    :param str cause_col: name of the column to group on. This can technically be a number, but that is uncommon.
    :param str result_col: column to do the boxplot on
    :param str pdf_file_name: if set, will store the plot in a pdf file
    :param float ylim_quant: the quantile of the y upper limit
    :param float ylim_high: when defined, this limit is used, when not defined, defaults to None and ylim_high is
           determined by ylim_quant
    :param float ylim_low: matplotlib set_ylim lower bound
    :param int rot: matplotlib rot
    :param int statlim: the number of entries that a group is required to have in order to be plotted
    :param dict label_dict: dictionary with labels for the columns, usage example: label_dict={'col_x': 'Time'}
    :param str title_add: string that is added to the automatic title (the y column name)
    :param int top: only print the top 20 characters of x-labels and y-labels. (default is 20)
    """
    # import matplotlib here to prevent import before setting backend in
    # core.execution.eskapade_run
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    # Check the number of categories in the cause_col, if this is too large, only plot the top 20.
    if len(df[cause_col].unique()) > top:
        top_x = df[cause_col].value_counts()[:top].index
        df = df[df[cause_col].isin(top_x)]
        logger.warning('The number of categories of column "{col}" is too large, boxplot is not generated.',
                       col=cause_col)

    # Build a figure
    fig = plt.figure(figsize=(8, 6))
    ax1 = fig.add_subplot(111)

    df_small, _ = delete_smallstat(df, cause_col, statlim=statlim)

    # Make boxplots
    df_small.boxplot(column=result_col, by=cause_col, ax=ax1, fontsize=20, rot=rot, grid=True)

    # If columns do not have a pretty name in label_dict, make the label the column name
    try:
        xlabel = label_dict[cause_col]
    except BaseException:
        xlabel = cause_col
    ax1.set_xlabel(xlabel, fontsize=20)

    try:
        title_label = label_dict[result_col] + title_add
    except BaseException:
        title_label = result_col + title_add
    ax1.set_title(title_label, fontsize=20)

    # Label parameters
    ax1.tick_params(axis='both', which='major', labelsize=20)
    subfig = ax1.get_figure()
    subfig.suptitle('')

    # Calculate quantile for y axis limit
    if ylim_high is None:
        ylim_high = df[result_col].dropna().quantile(q=ylim_quant)

    ax1.set_ylim(ylim_low, ylim_high)

    # Put the number of entries in each group as a number above each boxplot
    num_boxes = len(df_small[cause_col].unique())
    pos = np.arange(num_boxes) + 1
    sizes = list(df_small.groupby(cause_col).size())
    upper_labels = [str(np.round(s, 2)) for s in sizes]
    weights = ['bold', 'semibold']
    for tick in range(num_boxes):
        k = tick % 2
        ax1.text(pos[tick], ylim_high - (ylim_high * 0.05), upper_labels[tick], horizontalalignment='center',
                 size='larger', weight=weights[k])

    # 4. store plot
    if pdf_file_name:
        pdf_file = PdfPages(pdf_file_name)
        plt.savefig(
            pdf_file,
            format='pdf',
            bbox_inches='tight',
            pad_inches=0)
        plt.close()
        pdf_file.close()


def plot_correlation_matrix(matrix_colors, x_labels, y_labels, pdf_file_name='',
                            title='correlation', vmin=-1, vmax=1, color_map='RdYlGn', x_label='', y_label='', top=20,
                            matrix_numbers=None, print_both_numbers=True):
    """Create and plot correlation matrix.

    :param matrix_colors: input correlation matrix
    :param list x_labels: Labels for histogram x-axis bins
    :param list y_labels: Labels for histogram y-axis bins
    :param str pdf_file_name: if set, will store the plot in a pdf file
    :param str title: if set, title of the plot
    :param float vmin: minimum value of color legend (default is -1)
    :param float vmax: maximum value of color legend (default is +1)
    :param str x_label: Label for histogram x-axis
    :param str y_label: Label for histogram y-axis
    :param str color_map: color map passed to matplotlib pcolormesh. (default is 'RdYlGn')
    :param int top: only print the top 20 characters of x-labels and y-labels. (default is 20)
    :param matrix_numbers: input matrix used for plotting numbers. (default it matrix_colors)
    """
    # basic matrix checks
    assert matrix_colors.shape[0] == len(y_labels), 'matrix_colors shape inconsistent with number of y-labels'
    assert matrix_colors.shape[1] == len(x_labels), 'matrix_colors shape inconsistent with number of x-labels'
    if matrix_numbers is None:
        matrix_numbers = matrix_colors
        print_both_numbers = False  # only one set of numbers possible
    else:
        assert matrix_numbers.shape[0] == len(y_labels), 'matrix_numbers shape inconsistent with number of y-labels'
        assert matrix_numbers.shape[1] == len(x_labels), 'matrix_numbers shape inconsistent with number of x-labels'

    # import matplotlib here to prevent import before setting backend in
    # core.execution.eskapade_run
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib import colors

    fig, ax = plt.subplots(figsize=(7, 5))
    # cmap = 'RdYlGn' #'YlGn'
    norm = colors.Normalize(vmin=vmin, vmax=vmax)
    img = ax.pcolormesh(matrix_colors, cmap=color_map, edgecolor='w', linewidth=1, norm=norm)

    # set x-axis properties
    def tick(lab):
        """Get tick."""
        if isinstance(lab, (float, int)):
            lab = 'NaN' if np.isnan(lab) else '{0:.1f}'.format(lab)
        lab = str(lab)
        if len(lab) > top:
            lab = lab[:17] + '...'
        return lab

    # reduce default fontsizes in case too many labels?
    nlabs = max(len(y_labels), len(x_labels))
    fontsize_factor = 1
    if nlabs >= 10:
        fontsize_factor = 0.55
    if nlabs >= 20:
        fontsize_factor = 0.25

    # make plot look pretty
    ax.set_title(title, fontsize=14 * fontsize_factor)
    ax.set_yticks(np.arange(len(y_labels)) + 0.5)
    ax.set_xticks(np.arange(len(x_labels)) + 0.5)
    ax.set_yticklabels([tick(lab) for lab in y_labels], rotation='horizontal', fontsize=10 * fontsize_factor)
    ax.set_xticklabels([tick(lab) for lab in x_labels], rotation='vertical', fontsize=10 * fontsize_factor)
    if x_label:
        ax.set_xlabel(x_label, fontsize=12 * fontsize_factor)
    if y_label:
        ax.set_ylabel(y_label, fontsize=12 * fontsize_factor)

    fig.colorbar(img)

    # annotate with correlation values
    numbers_set = [matrix_numbers] if not print_both_numbers else [matrix_numbers, matrix_colors]
    for i, _ in enumerate(x_labels):
        for j, _ in enumerate(y_labels):
            point_color = float(matrix_colors[j][i])
            white_cond = (point_color < 0.7 * vmin) or (point_color >= 0.7 * vmax) or np.isnan(point_color)
            y_offset = 0.5
            for m, matrix in enumerate(numbers_set):
                if print_both_numbers:
                    if m == 0:
                        y_offset = 0.7
                    elif m == 1:
                        y_offset = 0.25
                point = float(matrix[j][i])
                if np.isnan(point):
                    label = 'NaN'
                elif print_both_numbers and m == 0: # number of entries
                    pointi = int(matrix[j][i])
                    label = '{0:d}'.format( pointi )
                else:
                    label = '{0:.2f}'.format(point)
                color = 'w' if white_cond else 'k'
                ax.annotate(label, xy=(i + 0.5, j + y_offset), color=color, horizontalalignment='center',
                            verticalalignment='center', fontsize=10 * fontsize_factor)

    # save plot in file
    if pdf_file_name:
        pdf_file = PdfPages(pdf_file_name)
        plt.savefig(pdf_file, format='pdf', bbox_inches='tight', pad_inches=0)
        plt.close()
        pdf_file.close()
    else:
        plt.show()


def plot_overlay_histogram(hists, x_label, y_label=None, hist_names=[],
                           is_num=True, is_ts=False, pdf_file_name='',
                           top=20, width_in=None, xlim=None):
    """Create and plot overlapping histograms of column values.

    :param hists: list of input numpy histogram = values, bin_edges
    :param str x_label: Label for histogram x-axis
    :param str y_label: Label for histogram y-axis
    :param bool is_num: True if observable to plot is numeric
    :param bool is_ts: True if observable to plot is a timestamp
    :param str pdf_file_name: if set, will store the plot in a pdf file
    :param int top: only print the top 20 characters of x-labels and y-labels. (default is 20)
    :param float width_in: the width of the bars of the histogram in percentage (0-1). Optional.
    :param tuple xlim: set the x limits of the current axes. Optional.
    """
    # import matplotlib here to prevent import before setting backend in
    # core.execution.eskapade_run
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    plt.figure(figsize=(7, 5))
    alpha = 1 / len(hists)
    for i, hist in enumerate(hists):
        try:
            hist_values = hist[0]
            hist_bins = hist[1]
        except BaseException:
            raise ValueError('Cannot extract binning and values from input histogram')

        assert hist_values is not None and len(
            hist_values), 'Histogram bin values have not been set.'
        assert hist_bins is not None and len(
            hist_bins), 'Histogram binning has not been set.'

        # basic attribute check: time stamps treated as numeric.
        if is_ts:
            is_num = True

        # plot numeric and time stamps
        if is_num:
            bin_edges = hist_bins
            bin_values = hist_values
            assert len(bin_edges) == len(bin_values) + 1, \
                'bin edges (+ upper edge) and bin values have inconsistent lengths: {:d} vs {:d}. {}'\
                .format(len(bin_edges), len(bin_values), x_label)

            if is_ts:
                # difference in seconds
                be_tsv = [pd.Timestamp(ts).value for ts in bin_edges]
                width = np.diff(be_tsv)
                # pd.Timestamp(ts).value is in ns
                # maplotlib dates have base of 1 day
                width = width / NUM_NS_DAY
            elif width_in:
                width = width_in
            else:
                width = np.diff(bin_edges)

            # plot histogram
            plt.bar(bin_edges[:-1], bin_values, width=width,
                    alpha=alpha, label=hist_names[i])

            # set x-axis properties
            if xlim:
                plt.xlim(xlim)
            else:
                plt.xlim(min(bin_edges), max(bin_edges))
            plt.xticks(fontsize=12, rotation=90 if is_ts else 0)
        # plot categories
        else:
            labels = hist_bins
            values = hist_values
            assert len(labels) == len(values), \
                'labels and values have different array lengths: {:d} vs {:d}. {}'.format(len(labels), len(values), x_label)

            # plot histogram
            tick_pos = np.arange(len(labels)) + 0.5
            plt.bar(tick_pos, values, width=0.8,
                    alpha=alpha, label=hist_names[i])

            # set x-axis properties
            def xtick(lab):
                """Get x-tick."""
                lab = str(lab)
                if len(lab) > top:
                    lab = lab[:17] + '...'
                return lab

            plt.xlim((0., float(len(labels))))
            plt.xticks(tick_pos, [xtick(lab)
                                  for lab in labels], fontsize=12, rotation=90)

        # set common histogram properties
    plt.xlabel(x_label, fontsize=14)
    plt.ylabel(
        str(y_label) if y_label is not None else 'Bin count',
        fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid()
    plt.legend()

    # store plot
    if pdf_file_name:
        pdf_file = PdfPages(pdf_file_name)
        plt.savefig(pdf_file, format='pdf', bbox_inches='tight', pad_inches=0)
        plt.close()
        pdf_file.close()


def plot_pair_grid(data, title, fpath, column_names=[], data2=None):

    """Plot a pairgrid for one or two datasets
    :param array data: Input data to plot
    :param str title: Title of the plot
    :param str fpath: if set, will store the plot in a pdf file
    :param list column_names: list of column names to be give to the plot
    :param array data2: second dataset to be plot in the pairgrid.
    """

    # import matplotlib here to prevent import before setting backend in
    # core.execution.eskapade_run
    import matplotlib.pyplot as plt
    # from matplotlib.backends.backend_pdf import PdfPages
    import seaborn as sns

    if data2 is not None:
        data = pd.DataFrame(data, columns=column_names)
        data['label'] = "Original"
        data_r = pd.DataFrame(data2, columns=column_names)
        data_r['label'] = 'Resampled'

        df = data.append(data_r)
    else:
        df = pd.DataFrame(data, columns=column_names)

    g = sns.PairGrid(data=df, hue='label', height=2)
    g.map_offdiag(plt.scatter, alpha=.1, s=3)
    g.map_diag(plt.hist, alpha=.5, edgecolor='w')
    g.add_legend()

    if fpath:
        # pdf_file = PdfPages(fpath)
        g.savefig(fpath, format='png', bbox_inches='tight', pad_inches=0, dpi=400)
        plt.close()
        # plt.close()
