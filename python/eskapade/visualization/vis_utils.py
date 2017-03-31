import numpy as np
import pandas as pd

NUM_NS_DAY = 24 * 3600 * int(1e9)


def plot_histogram(hist, x_label, y_label=None, is_num=True, is_ts=False,
                   pdf_file_name=''):
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
    except:
        raise Exception(
            'Cannot extract binning and values from input histogram')

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
            if len(lab) > 20:
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
    except:
        raise Exception(
            'Cannot extract ranges and grid from input histogram')

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
