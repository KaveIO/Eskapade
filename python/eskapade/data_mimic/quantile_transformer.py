import warnings
import numpy as np
from scipy import sparse
from scipy import stats

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils import check_array
from sklearn.utils.validation import (check_is_fitted, check_random_state, FLOAT_DTYPES)


BOUNDS_THRESHOLD = 1e-7


class QuantileTransformer(BaseEstimator, TransformerMixin):
    """Transform features using quantiles information.
    This method transforms the features to follow a uniform or a normal
    distribution. Therefore, for a given feature, this transformation tends
    to spread out the most frequent values. It also reduces the impact of
    (marginal) outliers: this is therefore a robust preprocessing scheme.
    The transformation is applied on each feature independently.
    The cumulative density function of a feature is used to project the
    original values. Features values of new/unseen data that fall below
    or above the fitted range will be mapped to the bounds of the output
    distribution. Note that this transform is non-linear. It may distort linear
    correlations between variables measured at the same scale but renders
    variables measured at different scales more directly comparable.
    Read more in the :ref:`User Guide <preprocessing_transformer>`.
    Parameters
    ----------
    n_quantiles : int, optional (default=1000)
        Number of quantiles to be computed. It corresponds to the number
        of landmarks used to discretize the cumulative density function.
    output_distribution : str, optional (default='uniform')
        Marginal distribution for the transformed data. The choices are
        'uniform' (default) or 'normal'.
    ignore_implicit_zeros : bool, optional (default=False)
        Only applies to sparse matrices. If True, the sparse entries of the
        matrix are discarded to compute the quantile statistics. If False,
        these entries are treated as zeros.
    subsample : int, optional (default=1e5)
        Maximum number of samples used to estimate the quantiles for
        computational efficiency. Note that the subsampling procedure may
        differ for value-identical sparse and dense matrices.
    random_state : int, RandomState instance or None, optional (default=None)
        If int, random_state is the seed used by the random number generator;
        If RandomState instance, random_state is the random number generator;
        If None, the random number generator is the RandomState instance used
        by np.random. Note that this is used by subsampling and smoothing
        noise.
    copy : boolean, optional, (default=True)
        Set to False to perform inplace transformation and avoid a copy (if the
        input is already a numpy array).
    Attributes
    ----------
    quantiles_ : ndarray, shape (n_quantiles, n_features)
        The values corresponding the quantiles of reference.
    references_ : ndarray, shape(n_quantiles, )
        Quantiles of references.
    Examples
    --------
    >>> import numpy as np
    >>> from sklearn.preprocessing import QuantileTransformer
    >>> rng = np.random.RandomState(0)
    >>> X = np.sort(rng.normal(loc=0.5, scale=0.25, size=(25, 1)), axis=0)
    >>> qt = QuantileTransformer(n_quantiles=10, random_state=0)
    >>> qt.fit_transform(X) # doctest: +ELLIPSIS
    array([...])
    See also
    --------
    quantile_transform : Equivalent function without the estimator API.
    StandardScaler : perform standardization that is faster, but less robust
        to outliers.
    RobustScaler : perform robust standardization that removes the influence
        of outliers but does not put outliers and inliers on the same scale.
    Notes
    -----
    For a comparison of the different scalers, transformers, and normalizers,
    see :ref:`examples/preprocessing/plot_all_scaling.py
    <sphx_glr_auto_examples_preprocessing_plot_all_scaling.py>`.
    """

    def __init__(self, n_quantiles=1000, output_distribution='uniform',
                 ignore_implicit_zeros=False, subsample=int(1e5),
                 random_state=None, copy=True):
        self.n_quantiles = n_quantiles
        self.output_distribution = output_distribution
        self.ignore_implicit_zeros = ignore_implicit_zeros
        self.subsample = subsample
        self.random_state = random_state
        self.copy = copy

    def _dense_fit(self, X, random_state):
        """Compute percentiles for dense matrices.
        Parameters
        ----------
        X : ndarray, shape (n_samples, n_features)
            The data used to scale along the features axis.
        """
        if self.ignore_implicit_zeros:
            warnings.warn("'ignore_implicit_zeros' takes effect only with"
                          " sparse matrix. This parameter has no effect.")

        n_samples, n_features = X.shape
        # for compatibility issue with numpy<=1.8.X, references
        # need to be a list scaled between 0 and 100
        references = (self.references_ * 100).tolist()
        self.quantiles_ = []
        for col in X.T:
            if self.subsample < n_samples:
                subsample_idx = random_state.choice(n_samples,
                                                    size=self.subsample,
                                                    replace=False)
                col = col.take(subsample_idx, mode='clip')
            self.quantiles_.append(np.percentile(col, references))
        self.quantiles_ = np.transpose(self.quantiles_)

    def _sparse_fit(self, X, random_state):
        """Compute percentiles for sparse matrices.
        Parameters
        ----------
        X : sparse matrix CSC, shape (n_samples, n_features)
            The data used to scale along the features axis. The sparse matrix
            needs to be nonnegative.
        """
        n_samples, n_features = X.shape

        # for compatibility issue with numpy<=1.8.X, references
        # need to be a list scaled between 0 and 100
        references = list(map(lambda x: x * 100, self.references_))
        self.quantiles_ = []
        for feature_idx in range(n_features):
            column_nnz_data = X.data[X.indptr[feature_idx]:
                                     X.indptr[feature_idx + 1]]
            if len(column_nnz_data) > self.subsample:
                column_subsample = (self.subsample * len(column_nnz_data) //
                                    n_samples)
                if self.ignore_implicit_zeros:
                    column_data = np.zeros(shape=column_subsample,
                                           dtype=X.dtype)
                else:
                    column_data = np.zeros(shape=self.subsample, dtype=X.dtype)
                column_data[:column_subsample] = random_state.choice(
                    column_nnz_data, size=column_subsample, replace=False)
            else:
                if self.ignore_implicit_zeros:
                    column_data = np.zeros(shape=len(column_nnz_data),
                                           dtype=X.dtype)
                else:
                    column_data = np.zeros(shape=n_samples, dtype=X.dtype)
                column_data[:len(column_nnz_data)] = column_nnz_data

            if not column_data.size:
                # if no nnz, an error will be raised for computing the
                # quantiles. Force the quantiles to be zeros.
                self.quantiles_.append([0] * len(references))
            else:
                self.quantiles_.append(
                    np.percentile(column_data, references))
        self.quantiles_ = np.transpose(self.quantiles_)

    def fit(self, X, y=None):
        """Compute the quantiles used for transforming.
        Parameters
        ----------
        X : ndarray or sparse matrix, shape (n_samples, n_features)
            The data used to scale along the features axis. If a sparse
            matrix is provided, it will be converted into a sparse
            ``csc_matrix``. Additionally, the sparse matrix needs to be
            nonnegative if `ignore_implicit_zeros` is False.
        Returns
        -------
        self : object
            Returns self
        """
        if self.n_quantiles <= 0:
            raise ValueError("Invalid value for 'n_quantiles': %d. "
                             "The number of quantiles must be at least one."
                             % self.n_quantiles)

        if self.subsample <= 0:
            raise ValueError("Invalid value for 'subsample': %d. "
                             "The number of subsamples must be at least one."
                             % self.subsample)

        if self.n_quantiles > self.subsample:
            raise ValueError("The number of quantiles cannot be greater than"
                             " the number of samples used. Got {} quantiles"
                             " and {} samples.".format(self.n_quantiles,
                                                       self.subsample))

        X = self._check_inputs(X)
        rng = check_random_state(self.random_state)

        # Create the quantiles of reference
        self.references_ = np.linspace(0, 1, self.n_quantiles,
                                       endpoint=True)
        if sparse.issparse(X):
            self._sparse_fit(X, rng)
        else:
            self._dense_fit(X, rng)

        return self

    def _transform_col(self, X_col, quantiles, inverse):
        """Private function to transform a single feature"""

        if self.output_distribution == 'normal':
            output_distribution = 'norm'
        else:
            output_distribution = self.output_distribution
        output_distribution = getattr(stats, output_distribution)

        # older version of scipy do not handle tuple as fill_value
        # clipping the value before transform solve the issue
        if not inverse:
            lower_bound_x = quantiles[0]
            upper_bound_x = quantiles[-1]
            lower_bound_y = 0
            upper_bound_y = 1
        else:
            lower_bound_x = 0
            upper_bound_x = 1
            lower_bound_y = quantiles[0]
            upper_bound_y = quantiles[-1]
            #  for inverse transform, match a uniform PDF
            X_col = output_distribution.cdf(X_col)
        # find index for lower and higher bounds
        lower_bounds_idx = (X_col - BOUNDS_THRESHOLD <
                            lower_bound_x)
        upper_bounds_idx = (X_col + BOUNDS_THRESHOLD >
                            upper_bound_x)

        if not inverse:
            # Interpolate in one direction and in the other and take the
            # mean. This is in case of repeated values in the features
            # and hence repeated quantiles
            #
            # If we don't do this, only one extreme of the duplicated is
            # used (the upper when we do assending, and the
            # lower for descending). We take the mean of these two
            X_col = .5 * (np.interp(X_col, quantiles, self.references_)
                          - np.interp(-X_col, -quantiles[::-1],
                                      -self.references_[::-1]))
        else:
            X_col = np.interp(X_col, self.references_, quantiles)

        X_col[upper_bounds_idx] = upper_bound_y
        X_col[lower_bounds_idx] = lower_bound_y
        # for forward transform, match the output PDF
        if not inverse:
            X_col = output_distribution.ppf(X_col)
            # find the value to clip the data to avoid mapping to
            # infinity. Clip such that the inverse transform will be
            # consistent
            clip_min = output_distribution.ppf(BOUNDS_THRESHOLD -
                                               np.spacing(1))
            clip_max = output_distribution.ppf(1 - (BOUNDS_THRESHOLD -
                                                    np.spacing(1)))
            X_col = np.clip(X_col, clip_min, clip_max)

        return X_col

    def _check_inputs(self, X, accept_sparse_negative=False):
        """Check inputs before fit and transform"""
        X = check_array(X, accept_sparse='csc', copy=self.copy,
                        dtype=[np.float64, np.float32])
        # we only accept positive sparse matrix when ignore_implicit_zeros is
        # false and that we call fit or transform.
        if (not accept_sparse_negative and not self.ignore_implicit_zeros and
                (sparse.issparse(X) and np.any(X.data < 0))):
            raise ValueError('QuantileTransformer only accepts non-negative'
                             ' sparse matrices.')

        # check the output PDF
        if self.output_distribution not in ('normal', 'uniform'):
            raise ValueError("'output_distribution' has to be either 'normal'"
                             " or 'uniform'. Got '{}' instead.".format(
                                 self.output_distribution))

        return X

    def _check_is_fitted(self, X):
        """Check the inputs before transforming"""
        check_is_fitted(self, 'quantiles_')
        # check that the dimension of X are adequate with the fitted data
        if X.shape[1] != self.quantiles_.shape[1]:
            raise ValueError('X does not have the same number of features as'
                             ' the previously fitted data. Got {} instead of'
                             ' {}.'.format(X.shape[1],
                                           self.quantiles_.shape[1]))

    def _transform(self, X, inverse=False):
        """Forward and inverse transform.
        Parameters
        ----------
        X : ndarray, shape (n_samples, n_features)
            The data used to scale along the features axis.
        inverse : bool, optional (default=False)
            If False, apply forward transform. If True, apply
            inverse transform.
        Returns
        -------
        X : ndarray, shape (n_samples, n_features)
            Projected data
        """

        if sparse.issparse(X):
            for feature_idx in range(X.shape[1]):
                column_slice = slice(X.indptr[feature_idx],
                                     X.indptr[feature_idx + 1])
                X.data[column_slice] = self._transform_col(
                    X.data[column_slice], self.quantiles_[:, feature_idx],
                    inverse)
        else:
            for feature_idx in range(X.shape[1]):
                X[:, feature_idx] = self._transform_col(
                    X[:, feature_idx], self.quantiles_[:, feature_idx],
                    inverse)

        return X

    def transform(self, X):
        """Feature-wise transformation of the data.
        Parameters
        ----------
        X : ndarray or sparse matrix, shape (n_samples, n_features)
            The data used to scale along the features axis. If a sparse
            matrix is provided, it will be converted into a sparse
            ``csc_matrix``. Additionally, the sparse matrix needs to be
            nonnegative if `ignore_implicit_zeros` is False.
        Returns
        -------
        Xt : ndarray or sparse matrix, shape (n_samples, n_features)
            The projected data.
        """
        X = self._check_inputs(X)
        self._check_is_fitted(X)

        return self._transform(X, inverse=False)

    def inverse_transform(self, X):
        """Back-projection to the original space.
        Parameters
        ----------
        X : ndarray or sparse matrix, shape (n_samples, n_features)
            The data used to scale along the features axis. If a sparse
            matrix is provided, it will be converted into a sparse
            ``csc_matrix``. Additionally, the sparse matrix needs to be
            nonnegative if `ignore_implicit_zeros` is False.
        Returns
        -------
        Xt : ndarray or sparse matrix, shape (n_samples, n_features)
            The projected data.
        """
        X = self._check_inputs(X, accept_sparse_negative=True)
        self._check_is_fitted(X)

        return self._transform(X, inverse=True)