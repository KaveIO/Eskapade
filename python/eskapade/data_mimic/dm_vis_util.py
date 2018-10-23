# import numpy as np
import pandas as pd


def plot_heatmap(df1, df2, titles=['Original', 'Resampled'], pdf_file_name=None, ):
    import seaborn as sns
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    assert type(df1) == pd.DataFrame, f"Input needs to be a pandas DataFrame, is {type(df1)}"
    assert type(df2) == pd.DataFrame, f"Input needs to be a pandas DataFrame, is {type(df2)}"

    if len(df1.columns) > len(df2.columns):
        df1 = df1[df2.columns]
    else:
            df2 = df2[df1.columns]

    corr = [df1.corr(), df2.corr()]

    plt.rcParams['figure.figsize'] = (12, 5)
    fig, axn = plt.subplots(1, 2, sharex=True, sharey=True)
    cbar_ax = fig.add_axes([.91, .3, .03, .4])

    # cmap = sns.diverging_palette(-1, 1, as_cmap=True)

    for i, ax in enumerate(axn.flat):
        sns.heatmap(corr[i], ax=ax,
                    cbar=i == 0,
                    cbar_ax=None if i else cbar_ax,
                    annot=True)
        ax.set_title(titles[i])

    # store plot
    if pdf_file_name:
        pdf_file = PdfPages(pdf_file_name)
        plt.savefig(pdf_file, format='pdf', bbox_inches='tight', pad_inches=0)
        plt.close()
        pdf_file.close()
    else:
        plt.show()
