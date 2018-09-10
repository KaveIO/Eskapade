import numpy as np
from sklearn.preprocessing import scale, QuantileTransformer
import string
import scipy
import pandas as pd


def generate_unordered_categorical_random_data(n_obs, p, dtype=np.str):
    """
    Generates unordered categorical random data.

    :param int n_obs: Number of data points (rows) to generate
    :param np.2darray p: The probabilities associated with each category per dimension. The length of p determines the
                         number of dimensions, the length of the j-th element of p (np.array) is the number of
                         categories for dimension j and p[j] are the probabilities for the categories of dimension j.
    :param type dtype: The type of the data (str or int)
    :return: The generated data
    :rtype: np.ndarray
    """
    n_dim = p.shape[0]
    alphabet = np.array(list(string.ascii_lowercase))

    data = np.empty((n_obs, n_dim), dtype=dtype)
    for j in range(n_dim):
        if dtype == np.str:
            data[:, j] = alphabet[np.random.choice(np.arange(0, len(p[j])), n_obs, p=p[j])]
        elif dtype == np.int:
            data[:, j] = np.random.choice(np.arange(0, len(p[j])), n_obs, p=p[j])
        else:
            raise NotImplementedError

    return data


def generate_ordered_categorical_random_data(n_obs, p):
    """
    Generates ordered categorical random data

    :param int n_obs: Number of data points (rows) to generate
    :param np.2darray p: The probabilities associated with each category per dimension. The length of p determines the
                         number of dimensions, the length of the j-th element of p (np.array) is the number of
                         categories for dimension j and p[j] are the probabilities for the categories of dimension j.
    :return: The generated data
    :rtype: np.ndarray
    """
    n_dim = p.shape[0]

    data = np.empty((n_obs, n_dim), dtype=np.int)
    for j in range(n_dim):
        data[:, j] = np.random.choice(np.arange(0, len(p[j])), n_obs, p=p[j])

    return data


def generate_continuous_random_data(n_obs, means_stds):
    """
    Generates continuous random data. The data is pulled from a gaussian distribution plus a uniform distribution.
    The lower and upper boundery of the uniform distribution are equal to -m - 5*s and m + 5*s respectively, where m
    and s are the mean and standard deviation of the gaussian distribution respectively.

    NB: m should be positive!

    :param int n_obs: Number of data points (rows) to generate
    :param np.2darray means_stds: The length of means_stds determines the number of dimensions. means_stds[0]
                                  (np.array) are the means for each dimension. means_stds[1] (np.array) are the
                                  standard deviations for each dimension
    :return: The generated data
    :rtype: np.ndarray
    """
    means = means_stds[0]
    stds = means_stds[1]
    try:
        assert len(means) == len(stds)
    except AssertionError:
        print('lenth of means is not equal to lenth of standard deviations')
    n_dim = len(means)

    data = np.empty((n_obs, n_dim), dtype=np.float)
    for j in range(n_dim):
        m = means[j]
        s = stds[j]
        data[:, j] = np.random.normal(m, s, n_obs) + np.random.uniform(low=-m - 5 * s, high=m + 5 * s, size=(n_obs,))

    return data


def generate_data(n_obs, p_unordered, p_ordered, means_stds, dtype_unordered_categorical_data=np.str):
    """
    Generates unordered categorical, ordered categorical and continuous random data.
    See the docs of the functions generate_unordered_categorical_random_data, generate_ordered_categorical_random_data
    and generate_continuous_random_data for more explanation about the p_unordered, p_ordered and means_stds
    parameters.

    :param int n_obs: Number of data points (rows) to generate
    :param np.2darray p_unordered: The probabilities associated with each category per unordered categorical dimension
    :param np.2darray p_ordered: The probabilities associated with each category per ordered categorical dimension
    :param np.2darray means_stds: The means and standard deviations for each dimension per continuous dimension
    :param type dtype_unordered_categorical_data: The type of the unordered categorical data (str or int)
    :return: The generated data
    :rtype: pd.DataFrame
    """
    unordered_categorical_data = generate_unordered_categorical_random_data(n_obs, p_unordered,
                                                                            dtype=dtype_unordered_categorical_data)
    ordered_categorical_data = generate_ordered_categorical_random_data(n_obs, p_ordered)
    continuous_data = generate_continuous_random_data(n_obs, means_stds)

    alphabet = np.array(list(string.ascii_lowercase))
    columns1 = list(alphabet[0:continuous_data.shape[1]])
    columns2 = list(alphabet[continuous_data.shape[1]:
                             continuous_data.shape[1] + unordered_categorical_data.shape[1]])
    columns3 = list(alphabet[continuous_data.shape[1] + unordered_categorical_data.shape[1]:
                             continuous_data.shape[1] + unordered_categorical_data.shape[1] +
                             ordered_categorical_data.shape[1]])

    df1 = pd.DataFrame(continuous_data, columns=columns1)
    df2 = pd.DataFrame(unordered_categorical_data, columns=columns2)
    df3 = pd.DataFrame(ordered_categorical_data, columns=columns3)
    df = pd.concat([df1, df2, df3], axis=1)

    return df


def find_peaks(data, continuous_i, count=1):
    """
    Finds peaks in a set of data points.

    :param np.ndarray data: the data
    :param iterable continuous_i: column indices. In these columns, this function searches for peaks.
    :param int count: the minimum number of data points with equal value for the value te be flagged as a peak
    :return: dict with column index-np.array with peak values as key-value pairs
    :rtype: dict
    """
    peaks = {}
    for d in continuous_i:
        u, c = np.unique(data[:, d], return_counts=True)
        peaks[d] = u[c > count]
    return peaks


def smooth_peaks(data, peaks, smoothing_fraction=0.0002):
    """

    :param np.ndarray data:
    :param dict peaks:
    :param float smoothing_fraction:
    :return:
    :rtype: np.ndarray
    """
    data_smoothed = data.copy()
    for d, vs in peaks.items():
        for v in vs:
            i = np.where(data[:, d] == v)[0]
            s = (data[:, 4].max() - data[:, d].min())*smoothing_fraction
            data_smoothed[i, 4] = np.random.normal(v, s, size=len(i))
    return data_smoothed


def remove_nans(data_smoothed):
    """

    :param np.ndarray data_smoothed:
    :return:
    :rtype: np.ndarray
    """
    data_no_nans = data_smoothed.copy()
    data_no_nans = data_no_nans[~np.isnan(data_no_nans).any(axis=1)]
    return data_no_nans


# add extreme values if you want to extend the range in which data is generated
def make_extremes(x, fraction=0.15):
    """

    :param x:
    :param float fraction:
    :return:
    :rtype: tuple
    """
    xmin = []
    xmax = []
    xdiff = []
    for i in range(x.shape[1]):
        y = x[...,i]
        y = y[~np.isnan(y)]
        xmin.append(np.min(y))
        xmax.append(np.max(y))
        xdiff.append((xmax[i]-xmin[i]))
    for i in range(x.shape[1]):
        if xmin[i] != 0:
            xmin[i] -= fraction * xdiff[i]
        xmax[i] += fraction * xdiff[i]
    return xmin, xmax


def append_extremes(data_continuous, fraction=0.15):
    """

    :param data_continuous:
    :param fraction:
    :return:
    :rtype:
    """
    xmin, xmax = make_extremes(data_continuous, fraction=fraction)
    data_extremes = np.append(data_continuous, [xmin, xmax], axis=0).copy()
    # save inidices, we want to remove the min and max after quantile transformation
    imin = np.argmin(data_extremes, axis=0)
    imax = np.argmax(data_extremes, axis=0)
    return data_extremes, imin, imax


def transform_to_normal(data_extremes, imin, imax):
    """
    Transforms a random distribution to a normal distribution in the following way:
    1. Compute the values of the CDF. These values are the percentiles. These are uniformly distributed.
    2. Use the percent point function (inverse of cdf) of a normal distribution to transform the uniform
       distribution to a normal distribution.

    :param data_extremes:
    :param imin:
    :param imax:
    :return:
    :rtype:
    """
    qts = []
    data_normalized_ = []
    for d in range(0, data_extremes.shape[1]):
        qt = QuantileTransformer(n_quantiles=len(data_extremes), subsample=len(data_extremes),
                                 output_distribution='normal', copy=True)
        a = qt.fit_transform(data_extremes[:, d].reshape(-1, 1))
        a = np.delete(a, np.array([imin[d], imax[d]]))
        data_normalized_.append(a)
        qts.append(qt)

    data_normalized = np.stack(data_normalized_, axis=-1)
    return data_normalized, qts


def kde_resample(n_resample, data, bw, variable_types, c_array):
    """

    :param n_resample:
    :param data:
    :param bw:
    :param variable_types:
    :param c_array:
    :return:
    :rtype:
    """
    # get dimensions
    n_obs = data.shape[0]
    data = data.reshape(n_obs, -1)
    n_dim = data.shape[1]

    # convert variable types
    variable_types_array = np.array(list(variable_types))

    # pick a random observation from the original data
    indices = np.random.choice(np.arange(n_obs), size=n_resample, replace=True)

    # get the original data for the indices
    resample = data[indices, :]

    for i in range(n_resample):
        for j in range(n_dim):
            if np.isnan(resample[i, j]):
                pass
            if variable_types_array[j] == 'c':
                resample[i, j] = np.random.normal(loc=resample[i, j], scale=bw[j])
            elif variable_types_array[j] == 'u':
                if np.random.rand() < bw[j]:
                    categories = c_array[j]
                    other_categories = categories[categories != resample[i, j]]
                    resample[i, j] = np.random.choice(other_categories)
            elif variable_types_array[j] == 'o':
                d = np.random.geometric(1 - bw[j]) - 1
                if np.random.rand() < .5:
                    d = -d
                resample[i, j] += d

    return resample, indices


def insert_back_nans(data_smoothed, data_normalized, data, unordered_categorical_i, ordered_categorical_i,
                     continuous_i):
    """
    Insert np.nan's back into the transformed continuous variables before resampling.

    :param data_smoothed:
    :param data_normalized:
    :param data:
    :param unordered_categorical_i:
    :param ordered_categorical_i:
    :param continuous_i:
    :return:
    :rtype:
    """
    data_continuous_nans = data_smoothed[:, continuous_i].copy()
    data_to_resample = []
    l = len(data)
    for d in range(0, data_normalized.shape[1]):
        i_nan = np.argwhere(np.isnan(data_continuous_nans[:, d]))
        i_not_nan = np.argwhere(~np.isnan(data_continuous_nans[:, d]))
        a = np.zeros(l)
        a.put(i_not_nan, data_normalized[:, d])
        a.put(i_nan, np.nan)
        data_to_resample.append(a)

    data_to_resample = np.stack(data_to_resample, axis=-1)
    data_to_resample = np.concatenate((data[:, unordered_categorical_i],
                                       data[:, ordered_categorical_i], data_to_resample),
                                       axis=1)
    return data_to_resample


def scale_and_invert_normal_transformation(resample_normalized_unscaled, continuous_i, qts):
    """

    :param resample_normalized_unscaled:
    :param continuous_i:
    :param qts:
    :return:
    :rtype:
    """
    resample = resample_normalized_unscaled.copy()
    i = 0
    for d in continuous_i:
        # scaling and inverting quantile transformation can only be done on the not NaN values
        i_not_nan = np.argwhere(~np.isnan(resample[:, d]))
        qt = qts[i]
        resample[i_not_nan, d] = qt.inverse_transform(scale(resample[i_not_nan, d]))
        i += 1
    return resample
