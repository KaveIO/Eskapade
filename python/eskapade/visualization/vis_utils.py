import numpy as np
import pandas as pd
import logging
import matplotlib.pyplot as plt

NUM_NS_DAY = 24 * 3600 * int(1e9)

log = logging.getLogger(__name__)


def plot_histogram(hist, x_label, y_label=None, is_num=True, is_ts=False, pdf_file_name='', top=20):
    """Create and plot histogram of column values

    :param hist: input numpy histogram = values, bin_edges
    :param str x_label: Label for histogram x-axis
    :param str y_label: Label for histogram y-axis
    :param bool is_num: True if observable to plot is numeric
    :param bool is_ts: True if observable to plot is a timestamp
    :param str pdf_file_name: if set, will store the plot in a pdf file
    """
    # import matplotlib here to prevent import before setting backend in
    # core.execution.run_eskapade
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    fig = plt.figure(figsize=(7, 5))

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
            'bin edges (+ upper edge) and bin values have inconsistent lengths: %d vs %d.' % \
            (len(bin_edges), len(bin_values))

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
            'labels and values have different array lengths: %d vs %d.' % \
            (len(labels), len(values))

        # plot histogram
        tick_pos = np.arange(len(labels)) + 0.5
        plt.bar(tick_pos - 0.4, values, width=0.8)

        # set x-axis properties
        def xtick(lab):
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
    """Plot 2d histogram with matplotlib

    :param hist: input numpy histogram = x_bin_edges, y_bin_edges, bin_entries_2dgrid
    :param tuple x_lim: range tuple of x-axis (min,max)
    :param tuple y_lim: range tuple of y-axis (min,max)
    :param str title: title of plot
    :param str x_label: Label for histogram x-axis
    :param str y_label: Label for histogram y-axis
    :param str pdf_file_name: if set, will store the plot in a pdf file
    """
    # import matplotlib here to prevent import before setting backend in
    # core.execution.run_eskapade
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    fig = plt.figure(figsize=(7, 5))

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
    """Remove low-statistics groups from data frame

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


def box_plot(df, cause_col, result_col='cost', ylim_quant=0.95, ylim_high=None, ylim_low=0, rot=90, statlim=400,
             label_dict=None, title_add='', top=20):
    """Make box plot

    Function that plots the boxplot of the column df[result_col] in groups of cause_col. This means that
    the DataFrame is grouped-by on the cause column and then the distribution per group is plotted in a boxplot
    using the standard pandas functionality.
    Boxplots with less than statlim (default=400 ) entries in it are automatically removed.

    :param df: pandas DataFrame
    :param str cause_col: name of the column to group on. This can technically be a number, but that is uncommon.
    :param str result_col: column to do the boxplot on
    :param float ylim_quant: the quantile of the y upper limit
    :param float ylim_high: when defined, this limit is used, when not defined, defaults to None and ylim_high is
           determined by ylim_quant
    :param float ylim_low: matplotlib set_ylim lower bound
    :param int rot: matplotlib rot
    :param int statlim: the number of entries that a group is required to have in order to be plotted
    :param dict label_dict: dictionary with labels for the columns, usage example: label_dict={'col_x': 'Time'}
    :param str title_add: string that is added to the automatic title (the y column name)
    """

    # Check the number of categories in the cause_col, if this is too large, only plot the top 20.
    if len(df[cause_col].unique()) > top:
        top_x = df[cause_col].value_counts()[:top].index
        df = df[df[cause_col].isin(top_x)]
        log.warning('The number of categories of column "%s" is too large, boxplot is not generated', cause_col)

    # Build a figure
    fig = plt.figure(figsize=(8, 6))
    ax1 = fig.add_subplot(111)

    df_small, n_removed = delete_smallstat(df, cause_col, statlim=statlim)

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
    for tick, label in zip(range(num_boxes), ax1.get_xticklabels()):
        k = tick % 2
        ax1.text(pos[tick], ylim_high - (ylim_high * 0.05), upper_labels[tick], horizontalalignment='center',
                 size='larger', weight=weights[k])
