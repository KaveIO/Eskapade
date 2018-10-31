import numpy as np


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


def plot_heatmaps(corr, x_labels, y_labels=None, vmin=-1, vmax=1, titles=['Original', 'Resampled'], color_map='RdYlGn',
                  pdf_file_name=None, top=20, x_label='', y_label='', matrix_numbers=None, print_both_numbers=True):

    """Create and plot two heatmaps with one colorbar.

    :param list corr: input correlation matrices in a list [corr1, corr2]
    :param list x_labels: Labels for histogram x-axis bins
    :param list y_labels: Labels for histogram y-axis bins. If none, equal to x_labels
    :param str pdf_file_name: if set, will store the plot in a pdf file
    :param list titles: title of the plots
    :param float vmin: minimum value of color legend (default is -1)
    :param float vmax: maximum value of color legend (default is +1)
    :param str x_label: Label for histogram x-axis
    :param str y_label: Label for histogram y-axis
    :param str color_map: color map passed to matplotlib pcolormesh. (default is 'RdYlGn')
    :param int top: only print the top 20 characters of x-labels and y-labels. (default is 20)
    :param matrix_numbers: input matrix used for plotting numbers. (default it matrix_colors)
    """
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib import colors

    norm = colors.Normalize(vmin=vmin, vmax=vmax)

    def tick(lab):
        """Get tick."""
        if isinstance(lab, (float, int)):
            lab = 'NaN' if np.isnan(lab) else '{0:.1f}'.format(lab)
        lab = str(lab)
        if len(lab) > top:
            lab = lab[:17] + '...'
        return lab

    if y_labels is None:
        y_labels = x_labels

    plt.rcParams['figure.figsize'] = (12, 5)
    fig, axn = plt.subplots(1, 2, sharex=True, sharey=True)
    cbar_ax = fig.add_axes([.91, .15, .03, .7])

    # cmap = sns.diverging_palette(-1, 1, as_cmap=True)

    for n, ax in enumerate(axn.flat):

        nlabs = max(len(y_labels), len(x_labels))
        fontsize_factor = 1
        if nlabs >= 10:
            fontsize_factor = 0.55
        if nlabs >= 20:
            fontsize_factor = 0.25
        img = ax.pcolormesh(corr[n], cmap=color_map, edgecolor='w', linewidth=1, norm=norm)
        ax.set_title(titles[n], fontsize=14 * fontsize_factor)
        ax.set_yticks(np.arange(len(y_labels)) + 0.5)
        ax.set_xticks(np.arange(len(x_labels)) + 0.5)
        ax.set_yticklabels([tick(lab) for lab in y_labels], rotation='horizontal', fontsize=10 * fontsize_factor)
        ax.set_xticklabels([tick(lab) for lab in x_labels], rotation='vertical', fontsize=10 * fontsize_factor)
        if x_label:
            ax.set_xlabel(x_label, fontsize=12 * fontsize_factor)
        if y_label:
            ax.set_ylabel(y_label, fontsize=12 * fontsize_factor)

        if n == 1:
            # only set the last plots colorbar
            fig.colorbar(img, cax=cbar_ax)

        # annotate with correlation values
        numbers_set = [matrix_numbers] if not print_both_numbers else [matrix_numbers, corr[n]]

        for i, _ in enumerate(x_labels):
            for j, _ in enumerate(y_labels):
                point_color = float(corr[n][j][i])
                white_cond = (point_color < 0.7 * vmin) or (point_color >= 0.7 * vmax) or np.isnan(point_color)
                y_offset = 0.5
                for m, matrix in enumerate(numbers_set):
                    if print_both_numbers:
                        if m == 0:
                            y_offset = 0.7
                        elif m == 1:
                            y_offset = 0.25
                    point = float(corr[n][j][i])
                    if np.isnan(point):
                        label = 'NaN'
                    elif print_both_numbers and m == 0: # number of entries
                        pointi = int(corr[n][j][i])
                        label = '{0:d}'.format( pointi )
                    else:
                        label = '{0:.2f}'.format(point)
                    color = 'w' if white_cond else 'k'
                    ax.annotate(label, xy=(i + 0.5, j + y_offset), color=color, horizontalalignment='center',
                                verticalalignment='center', fontsize=10 * fontsize_factor)

    # store plot
    if pdf_file_name:
        pdf_file = PdfPages(pdf_file_name)
        plt.savefig(pdf_file, format='pdf', bbox_inches='tight', pad_inches=0)
        plt.close()
        pdf_file.close()
    else:
        plt.show()
