import numpy as np
from sklearn import preprocessing
import string
import pandas as pd
import scipy


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


def generate_data(n_obs=None, p_unordered=None, p_ordered=None, means_stds=None,
                  dtype_unordered_categorical_data=np.str):
    """
    Generates unordered categorical, ordered categorical and continuous random data.
    See the docs of the functions generate_unordered_categorical_random_data, generate_ordered_categorical_random_data
    and generate_continuous_random_data for more explanation about the p_unordered, p_ordered and means_stds
    parameters.
    The column names are alphabetically ascending starting with the continuous columns, then the unordered
    categorical columns and finally the ordered categorical columns.

    :param int n_obs: Number of data points (rows) to generate
    :param np.2darray p_unordered: The probabilities associated with each category per unordered categorical dimension
    :param np.2darray p_ordered: The probabilities associated with each category per ordered categorical dimension
    :param np.2darray means_stds: The means and standard deviations per continuous dimension
    :param type dtype_unordered_categorical_data: The type of the unordered categorical data (str or int)
    :return: The generated data
    :rtype: pd.DataFrame
    """
    # Input checking
    assert n_obs is not None and n_obs != 0, 'n_obs is 0 or None'

    assert p_unordered is not None or p_ordered is not None or means_stds is not None, \
        'p_unordered is None, p_ordered is None and means_stds is None. Please set one of these values'

    if p_unordered is not None:
        unordered_categorical_data = generate_unordered_categorical_random_data(n_obs,
                                                                                p_unordered,
                                                                                dtype=dtype_unordered_categorical_data)
    else:
        unordered_categorical_data = np.array([[]])

    if p_ordered is not None:
        ordered_categorical_data = generate_ordered_categorical_random_data(n_obs, p_ordered)
    else:
        ordered_categorical_data = np.array([[]])

    if means_stds is not None:
        continuous_data = generate_continuous_random_data(n_obs, means_stds)
    else:
        continuous_data = np.array([[]])
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
    Finds peaks in a set of data points. A peak is a set of data points with more then 'count' data points with equal
    value.

    :param np.ndarray data: the data
    :param iterable continuous_i: column indices. In these columns, this function searches for peaks.
    :param int count: the minimum number of data points per unique value for the value te be flagged as a peak
    :return: (column index, np.array of peak values) as key-value pairs
    :rtype: dict
    """
    peaks = {}
    for d in continuous_i:
        u, c = np.unique(data[:, d], return_counts=True)
        peaks[d] = u[c > count]
    return peaks


def smooth_peaks(data, peaks, smoothing_fraction=0.0002):
    """
    Smooths the peaks in the data. All data points in the peaks (data points with equal value) are replaced by a sample
    drawn from a normal distribution. The mean of this normal distribution is equal to the value of the peak and the
    standard deviation of this normal distribution is equal to the smoothing_fraction times the range of the data (
    for the column holding the peak).

    :param np.ndarray data: the data
    :param dict peaks: (column index, np.array of peak values) as key-value pairs
    :param float smoothing_fraction: fraction of the range (of the column) to use for smoothing
    :return: smoothed data
    :rtype: np.ndarray
    """
    data_smoothed = data.copy()
    for d, vs in peaks.items():
        for v in vs:
            i = np.where(data[:, d] == v)[0]
            s = (data[:, d].max() - data[:, d].min()) * smoothing_fraction
            data_smoothed[i, d] = np.random.normal(v, s, size=len(i))
    return data_smoothed


def remove_nans(data_smoothed):
    """
    Removes np.nan from data_smoothed. If a row contains at least one np.nan then the whole row is removed.

    :param np.ndarray data_smoothed: the data
    :return: the data without np.nan's
    :rtype: np.ndarray
    """
    data_no_nans = data_smoothed.copy()
    data_no_nans = data_no_nans[~np.isnan(data_no_nans).any(axis=1)]
    return data_no_nans


def make_extremes(x, fraction=0.15):
    """
    Calculates extremes: extreme_max = max + fraction * range, extreme_min = min - fraction * range

    :param np.ndarray x: the data
    :param float fraction: fraction of range to add to min and max
    :return: extreme_min and extreme_max
    :rtype: tuple
    """
    xmin = []
    xmax = []
    xdiff = []
    for i in range(x.shape[1]):
        y = x[..., i]
        y = y[~np.isnan(y)]
        xmin.append(np.min(y))
        xmax.append(np.max(y))
        xdiff.append((xmax[i] - xmin[i]))
    for i in range(x.shape[1]):
        xmin[i] -= fraction * xdiff[i]
        xmax[i] += fraction * xdiff[i]
    return xmin, xmax


def append_extremes(data_continuous, fraction=0.15):
    """
    Appends extremes to the data.

    :param np.ndarray data_continuous: the data
    :param float fraction: fraction to use for calculation of the extremes
    :return: the extremes appended to data_continuous
    :rtype: tuple
    """
    xmin, xmax = make_extremes(data_continuous, fraction=fraction)
    # extremes are added because we want to extend the range on which the data is transformed to a normal distribution.
    data_extremes = np.append(data_continuous, [xmin, xmax], axis=0).copy()
    # save inidices, we want to remove the min and max after quantile transformation
    imin = np.argmin(data_extremes, axis=0)
    imax = np.argmax(data_extremes, axis=0)
    return data_extremes, imin, imax


def transform_to_normal(data_extremes, imin, imax):
    """
    Transforming a random distribution to a normal distribution can be done in the following way:

    1. Compute the values of the CDF. These values are the percentiles. These are (always) uniformly distributed.

    2. Use the percent point function (inverse of cdf) of a normal distribution to transform the uniform\
       distribution to a normal distribution.

    :param np.ndarray data_extremes: the continuous data columns with smoothed peaks and extremes appended
    :param np.array imin: indices of the minimum per continuous column
    :param np.array imax: indices of the maximum per continuous column
    :return: the continuous columns normalized
    :rtype: tuple (np.ndarray, list of trained sklearn.preprocessing.data.QuantileTransformer)
    """
    qts = []
    data_normalized_ = []
    for d in range(0, data_extremes.shape[1]):
        qt = preprocessing.QuantileTransformer(n_quantiles=len(data_extremes), subsample=len(data_extremes),
                                               output_distribution='normal', copy=True)
        a = qt.fit_transform(data_extremes[:, d].reshape(-1, 1))
        a = np.delete(a, np.array([imin[d], imax[d]]))
        data_normalized_.append(a)
        qts.append(qt)

    if data_normalized_:
        data_normalized = np.stack(data_normalized_, axis=-1)
    else:
        data_normalized = np.empty((0,0))
    return data_normalized, qts


def insert_back_nans(data_normalized, data, unordered_categorical_i, ordered_categorical_i,
                     continuous_i):
    """
    Insert np.nan's back into the transformed continuous variables (data_normalized) before resampling and concatenates
    to original unordered and ordered categorical columns (with np.nan's).

    :param np.ndarray data_normalized: continuous data (without nan's) and normalized
    :param np.ndarray data: the original data (with nan's)
    :param list unordered_categorical_i: indeces of the unordered categorical columns in the original data
    :param list ordered_categorical_i: indeces of the ordered categorical columns in the original data
    :param list continuous_i: indeces of the continuous columns in the original data
    :return: data ready for resampling
    :rtype: np.ndarray
    """
    data_continuous_nans = data[:, continuous_i].copy()
    data_to_resample = []
    l = len(data)
    if not data_normalized.size:
        data_to_resample = np.concatenate((data[:, unordered_categorical_i],
                                           data[:, ordered_categorical_i]), axis=1)
    else:
        for d in range(0, data_normalized.shape[1]):
            i_nan = np.argwhere(np.isnan(data_continuous_nans[:, d]))
            i_not_nan = np.argwhere(~np.isnan(data_continuous_nans[:, d]))
            a = np.zeros(l)
            a.put(i_not_nan, data_normalized[:, d])
            a.put(i_nan, np.nan)
            data_to_resample.append(a)

        data_to_resample = np.stack(data_to_resample, axis=-1)
        data_to_resample = np.concatenate((data[:, unordered_categorical_i],
                                          data[:, ordered_categorical_i], data_to_resample), axis=1)

    return data_to_resample


def kde_resample(n_resample, data, bw, variable_types, c_array):
    """
    Generates new data with length n_resample based on the input data. This function loops over all data points and
    draws a new data point from a kernel layed over the data point. The size of the kernel is determined by the
    bandwiths. The kernels are:
    - the Aitchison and Aitken kernel for unordered categorical columns
    - a symmetric geometric distribution for ordered categorical columns
    - a gaussian distribution for continuous columns

    If a data point is missing (NaN) a NaN is returned.
    If n_resample is larger then the input data, then some (randomly chosen) data points will be used twice.

    :param int n_resample: the size of the resample
    :param np.ndarray data: the input data used for resampling.
    :param np.array bw: the bandwiths per column from the kernel density estimation
    :param str variable_types: the type of the variables:
                                - c : continuous
                                - u : unordered (discrete)
                                - o : ordered (discrete)
                                The string should contain a type specified for each variable, for example
                                ``var_type='ccuo'``.
    :param list c_array: list of np.arrays containing all possible categories per uordered categorical dimension
    :return: the resampled data and the indices of the rows of the input data used to generate the resampled row.
    :rtype: tuple (np.ndarray, np.array)
    """
    # get dimensions
    n_obs = data.shape[0]
    data = data.reshape(n_obs, -1)
    n_dim = data.shape[1]

    # get z for cat

    # convert variable types
    variable_types_array = np.array(list(variable_types))

    # pick a random set from the original data
    indices = np.random.choice(np.arange(n_obs), size=n_resample, replace=True)

    # get the original data for the indices
    resample = data[indices, :]

    for i in range(n_resample):
        for j in range(n_dim):
            if np.isnan(resample[i, j]):
                pass
            elif variable_types_array[j] == 'c':
                resample[i, j] = np.random.normal(loc=resample[i, j], scale=bw[j])
            elif variable_types_array[j] == 'u':
                if np.random.rand() < bw[j]:
                    categories = c_array[j]
                    other_categories = categories[categories != resample[i, j]]
                    # [YW] this does not work if there are no other categories
                    # because they were not sampled or simply not present in the original dataset
                    if other_categories.size != 0:
                        resample[i, j] = np.random.choice(other_categories)
            elif variable_types_array[j] == 'o':
                # --  points at which the pdf should be evaluated
                z = c_array[j]
                p = wr_kernel(s=bw[j], z=z, zi=resample[i, j])
                try:
                    assert p.sum().round(3) == 1.0
                except AssertionError:
                    print(f"p must sum to 1!! {p.sum()}")
                    raise
                resample[i, j] = np.random.choice(a=z, p=p)

    return resample, indices


def scale_and_invert_normal_transformation(resample_normalized_unscaled, continuous_i, qts):
    """
    Transforms the resampled data back from normalized form.

    :param np.ndarray resample_normalized_unscaled: the data in normalized form
    :param list continuous_i: indeces of the continuous columns in the original data
    :param list qts: the trained trained sklearn.preprocessing.data.QuantileTransformer
    :return: the resample transformed back from normalized form
    :rtype: np.ndarray
    """
    resample = resample_normalized_unscaled.copy()
    i = 0
    for d in continuous_i:
        # scaling and inverting quantile transformation can only be done on the not NaN values
        i_not_nan = np.argwhere(~np.isnan(resample[:, d]))
        qt = qts[i]
        # todo:
        # test without scaling step. What is the effect of the scaling step? Does it bring the data closer to
        # the true distribution or to the one with statistical fluctuations (the original data)?
        resample[i_not_nan, d] = qt.inverse_transform(preprocessing.scale(resample[i_not_nan, d]))
        i += 1
    return resample


def wr_kernel(s, z, zi):
    r"""Implementation of the wang-ryzin kernel as implemented by statsmodels.

    Adapted so it is defined for finite intervals ex. {1...c}.

    The Wang-Ryzin kernel, used for ordered discrete random variables.

    Parameters

    s:   scalar or 1-D ndarray, shape (K,)
         The bandwidth(s) used to estimate the value of the kernel function
    zi:  ndarray of ints, shape (nobs, K)
         The value of observable(s)
    z:   scalar or 1-D ndarray of shape (K,)
         The value at which the kernel density is being estimated. For finite
         intervals, should be the possible values of the original dataset

    Returns

    kernel_value : ndarray, shape (nobs, K)
        The value of the kernel function at each training point for each var.

    Notes

    See p. 19 in Ref.1 for details.  The value of the kernel L if \
    :math:`X_{i}=x` is :math:`1-\lambda`, otherwise it is \
    :math:`\frac{1-\lambda}{2}\lambda^{|X_{i}-x|}`, where :math:`\lambda` is \
    the bandwidth.

    References

    .. [*] Racine, Jeff. "Nonparametric Econometrics: A Primer," Foundation \
           and Trends in Econometrics: Vol 3: No 1, pp1-88., 2008. \
           http://dx.doi.org/10.1561/0800000009
    .. [*] M.-C. Wang and J. van Ryzin, "A class of smooth estimators for \
           discrete distributions", Biometrika, vol. 68, pp. 301-309, 1981.
    """
    zi = zi.reshape(zi.size)  # seems needed in case Zi is scalar

    kernel_value = 0.5 * (1 - s) * (s ** abs(zi - z))

    # --  if Zi == z
    idx = zi == z
    kernel_value[idx] = (idx * (1 - s))[idx]

    corr_factor = kernel_value.sum()

    return kernel_value / corr_factor


def aitchison_aitken_kernel(l, c):
    """
    Calculates the values of the Aitchison-Aitken kernel

    :param np.array l: lambda, the bandwith to be evaluated against
    :param np.array c: the category
    :return: the value of the kernel evaluated at
    :rtype: np.ndarray
    """
    kernel_values = np.array([[l / (c - 1)], [1 - l]])

    return kernel_values


def aitchison_aitken_convolution(l, c):
    """
    Calculates the values of the Aitchison-Aitken convolutions

    :param np.array l: lambda, the bandwith to be evaluated against
    :param np.array c: integer, the category
    :return: the value of the kernel convolution at c for bandwith l
    :rtype: np.ndarray
    """
    l_1 = 1 - l
    l_0 = l / (c - 1)

    ll_00 = np.multiply(l_0, l_0)
    ll_10 = np.multiply(l_1, l_0)
    ll_11 = np.multiply(l_1, l_1)

    convolution_values = np.array([[((c - 2) * ll_00) + (2 * ll_10)], [((c - 1) * ll_00) + ll_11]])

    return convolution_values


def unorderd_mesh_kernel_values(l, c, n_dim):
    """
    Calculates all values of Aitchison-Aitken kernel for all possible delta vector combinations

    :param np.array l: lambda, the bandwith to be evaluated against
    :param np.array c: the category
    :param n_dim: integer, number of rows
    :return: the value of the kernel evaluated at the entire vector, i.e. the product of the seperate kernels
    :rtype: np.ndarray
    """
    # for each distance value per dimension calculate the kernel value
    kernel_values = aitchison_aitken_kernel(l, c)

    # place the calculated values back w.r.t. the distances array
    kernel_values_mesh = construct_meshgrid(
        np.split(np.ndarray.flatten(kernel_values).reshape(n_dim, 2, order='F'), n_dim))

    # take the product over all dimensions for each distances combination
    product_kernel_values = np.prod(kernel_values_mesh, axis=1)

    return product_kernel_values


def unorderd_mesh_convolution_values(l, c, n_dim):
    """
    Calculates all values of Aitchison-Aitken convolution for all possible delta vector combinations

    :param np.array l: lambda, the bandwith to be evaluated against
    :param np.array c: the category
    :param int n_dim: number of rows
    :return: the value of the kernel evaluated at the entire vector, i.e. the product of the seperate kernels
    :rtype: np.ndarray
    """
    # for each distance value per dimension calculate the kernel value
    convolution_values = aitchison_aitken_convolution(l, c)

    # place the calculated values back w.r.t. the distances array
    convolution_values_mesh = construct_meshgrid(
        np.split(np.ndarray.flatten(convolution_values).reshape(n_dim, 2, order='F'), n_dim))

    # take the product over all dimensions for each distances combination
    product_convolution_values = np.prod(convolution_values_mesh, axis=1)

    return product_convolution_values


def unordered_mesh_eval(l, c, n_obs, n_dim, delta_frequencies, cv_delta_frequencies):
    """
    Calculates the cross validation score for categorical optimization

    :param float l: lambda, the bandwith to be evaluated against
    :param int c: the category
    :param integer n_dim: number of features
    :param int n_obs: number of observations
    :param np.array delta_frequencies: frequency of the occurence of each unique (feature) vector
    :param np.array cv_delta_frequencies: frequency of the occurence of each unique (feature) vector
    :return: the cv score
    :rtype: np.ndarray
    """
    convolution_values = unorderd_mesh_convolution_values(l, c, n_dim)
    kernel_values = unorderd_mesh_kernel_values(l, c, n_dim)

    convolution_term = np.inner(convolution_values, delta_frequencies)
    kernel_term = np.inner(kernel_values, cv_delta_frequencies)

    cv = convolution_term / (n_obs ** 2) - (2 * kernel_term) / (n_obs * (n_obs - 1))

    return cv


def hash_combinations(hash_function, combinations):
    """
    Hash function

    :param np.array combinations: combinations on which to apply the hash function
    :param np.array hash_function: the result of the hash function
    :return: the inner product of the hash_function and the combinations
    :rtype: np.ndarray
    """
    return np.inner(combinations, hash_function)


def construct_meshgrid(array):
    """
    Gives the total enumeration of all possible values, where the values per dimension j are given in array[j]

    :param np.array array: combinations on which to apply the hash function
    :return: meshgrid, on which to evaluate the cv
    :rtype: np.ndarray
    """
    dimensions = np.array(array).shape[0]
    meshgrid_parameter_str = ''
    for j in range(dimensions):
        meshgrid_parameter_str += 'array[{}], '.format(j)

    meshgrid = eval('np.array(np.meshgrid(' + meshgrid_parameter_str + 'indexing=\'ij\')).reshape(dimensions,-1).T')

    return meshgrid


def calculate_delta_frequencies(data, n_obs, n_dim):
    """
    Calculates how often each difference delta=1 : X_{i_1} == X_{i_2} delta=0 : X_{i_1} != X_{i_2}
    appears in the comparison of all observations with each other {X_{i_1}}_{i_1=1}^n, {X_{i_2}}_{i_2=1}^n,

    :param np.array data: the data
    :param int n_obs: number of observations
    :param int n_dim: number of dimensions
    :return: the frequency of each delta vector
    :rtype: np.ndarray
    """
    # observed frequencies
    observerd_combinations, observed_frequencies = np.unique(data, axis=0, return_counts=True)

    # get all the indices of the pairs of data combinations to compare
    combinations_index_range = np.arange(observed_frequencies.size)
    combinations_index_comparison_meshgrid = construct_meshgrid(np.array([combinations_index_range,
                                                                          combinations_index_range])).astype(np.uint64)

    # per pair of data combinations get the delta vector
    comparisons_deltas = np.equal(observerd_combinations[combinations_index_comparison_meshgrid[:, 0], :],
                                  observerd_combinations[combinations_index_comparison_meshgrid[:, 1], :]).astype(
        np.uint8)

    # hash the delta combinations;
    delta_combination_hash = hash_combinations(np.power(2, np.arange(n_dim - 1, -1, -1)), comparisons_deltas)

    # per pair of category combinations get the frequency
    combination_product_frequencies = np.multiply(observed_frequencies[combinations_index_comparison_meshgrid[:, 0]],
                                                  observed_frequencies[combinations_index_comparison_meshgrid[:, 1]])

    delta_frequencies = np.zeros(2 ** n_dim, dtype=np.uint64)

    np.add.at(delta_frequencies, delta_combination_hash, combination_product_frequencies)

    cv_delta_frequencies = np.array(delta_frequencies, copy=True)
    cv_delta_frequencies[
        hash_combinations(np.power(2, np.arange(n_dim - 1, -1, -1)), np.ones(n_dim)).astype(int)] -= n_obs

    return delta_frequencies, cv_delta_frequencies


def kde_only_unordered_categorical(data):
    """Return the optimal combinations of dimensional bandwidths

    Given the a dataset consisting of only categorical variables,
    return the optimal combinations of dimensional bandwidths.

    :param np.array data: data
    :return: the optimal bandwiths
    :rtype: list

    References:

    .. [*] van Bokhorst, P.S. \"Data Set Resampling with Multivariate Mixed Variable Kernel Density Estimation\"
    """
    # determine data settings
    n_obs = data.shape[0]
    data = data.reshape(n_obs, -1)  # if data is one-dimensional
    n_dim = data.shape[1]
    c = np.amax(data, axis=0) + 1

    delta_frequencies, cv_delta_frequencies = calculate_delta_frequencies(data, n_obs, n_dim)

    opt = scipy.optimize.differential_evolution(unordered_mesh_eval,
                                                bounds=[(0, (c[j] - 1) / c[j]) for j in range(n_dim)],
                                                args=(c, n_obs, n_dim, delta_frequencies, cv_delta_frequencies),
                                                tol=1e-10)
    return opt.x


def scaled_chi(o, e, k=None):
    """
    Function to calculate a weighted chi square for two
    binned distributions, o (observerd) and e (expected).
    Based on https://www.itl.nist.gov/div898/software/dataplot/refman1/auxillar/chi2samp.htm

    :param array o: Observed frequencies
    :param array e: Expected frequencies
    :param array k: bins == dof
    :return: Chi, p-value
    :rtype: float, float
        """
    assert len(o) == len(e), f"Both samples need to have the same amount of bins! {len(o), len(e)}"
    if k is None:
        k = len(o)
    # calc scaling constants:
    ko = np.sqrt(np.sum(e) / np.sum(o))
    ke = np.sqrt(np.sum(o) / np.sum(e))

    if ko == ke == 1:
        dof = k - 1
    else:
        dof = k

    chi = np.sum(((ko * o - ke * e)**2) / (e + o))
    p = 1 - scipy.stats.chi2.cdf(chi, dof)
    return chi, p


def map_random(a):
    """
    Hashes a column

    :param np.array a: the column to be hashed
    :return: The hashed column
    :rtype: np.array
    """

    if type(a[0]) == np.float64:
        temp = np.zeros(shape=len(a))
        key = np.unique(a)
        value = np.array(list(range(len(key)))).astype(np.float64)
        np.random.shuffle(value)

    if type(a[0]) == np.str:
        temp = np.zeros(shape=len(a))
        key = np.unique(a)
        value = np.array(list(string.ascii_lowercase[0:len(key)])).astype(np.str)
        np.random.shuffle(value)

    mapper = dict(zip(key, value))
    for k, v in mapper.items():
        temp[a == k] = v

    return temp


def column_hashing(data, columns_to_hash, randomness, column_names):
    """
    Hashes the columns in the data to a random other value.

    :param np.2darray data: The data
    :param list columns_to_hash: The names of the columns which are to be hashed
    :param int randomness: A cryptographically random int
    :param np.array column_names: array with the column\
    names of the data, used for indexing so order must be correct
    :return: The hashed data
    :rtype: np.2dndarray
    """

    np.random.seed(randomness)

    for column in columns_to_hash:
        column_number = column_names.index(column)
        data[:, column_number] = map_random(data[:, column_number])

    return data
