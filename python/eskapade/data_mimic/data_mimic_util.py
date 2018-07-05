import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as sql_funcs
import scipy.special
import scipy.stats
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler


def group_distance(elements, dist):
    """Group all elements within a certain distance from each other

    For each element, identify which other elements are within 'dist'.
    Subsequently, construct the corresponding groups of elements.
    N.B. Function breaks down if elements contain duplicates.
    This can simply be prevented by adding an arbitrary small number,
    without affecting constructed groups (removing duplicates is also
    possible, but was undesirable in the context of applying
    create_features_clusters.py to current datasets for Salt-project)

    :param list elements: elements for which to evaluate grouping
    :param float dist: distance threshold within which to group elements
    :return: list groups: list of identified groups with all member elements
    """

    # for each element, determine which other elements are within distance
    connections = dict()
    visited = dict()
    for element in elements:
        connections[element] = []
        visited[element] = False
        for another in elements:
            if abs(element - another) <= dist:
                connections[element].append(another)

    # construct identified groups by recursively adding all elements within dist
    # N.B. use 'visited'-parameter to indicate that an element has been evaluated
    # (otherwise this procedure will result in an infinite loop)
    groups = []
    for ie, element in enumerate(elements):
        if not visited[element]:
            group = []
            group = group_elements_index(ie, element, group, visited, connections)
            groups.append(group)
    return groups


def group_elements_index(k, element, group, visited, connections):
    """Identify points within a given distance from element

    This implementation returns indices of elements in group

    :param int k: index of element for which to evaluate distance
    :param int|float|str element: element for which to evaluate distance
    :param list group: group of identified neighbouring elements
    :param visited: whether or not an element has been evaluated
    :param connections: for each element, indicates other close-by elements
    :return: list group: group of identified neighbouring elements
    """
    if visited[element]:
        return
    visited[element] = True
    group.append(k)
    for j, another in enumerate(connections[element]):
        group = group_elements_index(j, another, group, visited, connections)
    return group


def group_elements(element, group, visited, connections):
    """Identify points within a given distance from element

    This implementation returns values of elements in group

    :param int|float|str element: element for which to evaluate distance
    :param list group: group of identified neighbouring elements
    :param visited: whether or not an element has been evaluated
    :param connections: for each element, indicates other close-by elements
    :return: list group: group of identified neighbouring elements
    """

    if visited[element]:
        return
    visited[element] = True
    group.append(element)
    for another in connections[element]:
        group = group_elements(another, group, visited, connections)
    return group


def create_features_clusters(group, feature_param, variations):
    """Create features for identified clusters (stable periods) at regular intervals

    :param ResultIterable group: 3-d tuple with cluster_start, duration and median value for a group (tag)
    :param dict feature_param: parameters required to construct features
    :param pandas_series variations: Pandas series with typical variations for a tag
    :return: list_out: list of tuples with features per interval (timestamp) for a group (tag)
    """

    # reshape input
    list_data = list(zip(*list(group[1])))

    # extract tag_id from first row of list (grouping by tag, so tag_id is the same for each row in the group)
    tag = group[0]

    # construct pandas dataframe
    df = pd.DataFrame(data={'tag_id': tag,
                            'cluster_start': pd.to_datetime(np.array(list_data[0]) * 1e9),
                            'duration_minutes': list_data[1],
                            'median': list_data[2]})

    df['duration_timedelta'] = df['duration_minutes'].apply(lambda x: pd.Timedelta(x, 'm'))
    df['cluster_end'] = df['cluster_start'] + df['duration_timedelta']

    df = df.sort_values(by=['tag_id', 'cluster_start'])

    timewindow = {'min': pd.Timestamp(feature_param['timewindow']['min']),
                  'max': pd.Timestamp(feature_param['timewindow']['max'])}

    # set width of time interval
    nhours = feature_param['interval_nhours']
    timeperiod = pd.Timedelta('{0:d}h'.format(nhours))
    intervals = pd.date_range(start=timewindow['min'], end=timewindow['max'], freq=timeperiod, closed='left')

    # minimum total duration of similar-valued stable period(s) required in an interval to set the interval value
    duration_frac_limit = feature_param['duration_frac_limit']

    # scaling of typical variation to group stable periods
    dist_scale = feature_param['dist_scale']

    # initialize empty dataframe to fill with stable periods
    df_features = pd.DataFrame(intervals, columns=['interval_ts'])
    columns = ['tag_id', 'interval_value', 'total_stable_duration', 'final_stable_duration', 'n_clusters',
               'spread_value']
    for item in columns:
        df_features[item] = np.nan
    df_features = df_features.sort_values(by='interval_ts')
    df_features['interval_ordinal'] = ((df_features['interval_ts'] - timewindow['min']) / timeperiod + 1).astype(int)
    df_features = df_features.set_index('interval_ordinal').copy()

    # only consider the intervals for which a tag has a stable period
    # (leave the value at NaN for those intervals that do not contain stable periods)
    # N.B. possibility to merge this functionality of value-setting with flagging of stable intervals
    df_tmp = df.copy()
    df_tmp.set_index('tag_id', inplace=True)

    df_tmp['intervalFirst'] = np.ceil((df_tmp['cluster_start'] - timewindow['min']) / timeperiod).astype(int)
    df_tmp['intervalLast'] = np.ceil((df_tmp['cluster_start'] + df_tmp['duration_timedelta'] -
                                      timewindow['min']) / timeperiod).astype(int)

    if tag in variations.index:
        typical_var = variations.loc[tag]
    else:
        # set a very small value for tags for which no typical variation was determined
        # N.B. this is probably because there is no variation in such tags
        typical_var = 1e-8

    dist = dist_scale * typical_var

    intervalSet = []
    for irow, row in df_tmp.loc[[tag]].iterrows():
        intervalSet = np.append(intervalSet, np.arange(row['intervalFirst'], row['intervalLast'] + 1))
    intervalSet_count = [[int(x), list(intervalSet).count(x)] for x in set(intervalSet)]

    # determine number of stable periods in each interval per tag
    intervalCount = {k: v for k, v in dict(intervalSet_count).items() if v > 0}

    j = 0
    duration_count = 0
    for k, v in intervalCount.items():

        vals = df_tmp[(df_tmp['intervalFirst'] <= k) & (df_tmp['intervalLast'] >= k)].loc[tag]['median']
        if type(vals) == np.float64:
            elements = [vals]
        else:
            elements = list(vals)
        if np.any(np.diff(elements) == 0):
            # group_distance breaks down if elements consists of multiple values that are the same
            # prevent this by adding small number here, does not affect constructed features
            elements += np.arange(0, len(elements)) * 1e-9

        # calculate total amount of stable period in interval (regardless of value, as done for df_duration)
        overlap_all = 0  # pd.Timedelta('')
        dfcls = df_tmp[(df_tmp['intervalFirst'] <= k) & (df_tmp['intervalLast'] >= k)].loc[[tag]].copy()
        for i in range(0, len(elements)):
            max_edge = max(df_features.loc[k]['interval_ts'], dfcls.iloc[i]['cluster_start'])
            min_edge = min(df_features.loc[k]['interval_ts'] + timeperiod, dfcls.iloc[i]['cluster_end'])
            overlap_all += int((min_edge - max_edge).value / 1e9 / 60)

        df_features.loc[k, 'tag_id'] = tag
        df_features.loc[k, 'total_stable_duration'] = overlap_all
        df_features.loc[k, 'n_clusters'] = len(elements)
        df_features.loc[k, 'spread_value'] = np.max(elements) - np.min(elements)

        groups = group_distance(elements, dist)
        if len(groups) != 1:
            # if this is true, difference in value between multiple stable periods in the interval is larger than dist
            # October, 2017: leave these intervals as NaN (i.e. assume that they are not stable after all)
            # Main caveats: these different-valued stable periods could be interesting AND
            # if 90% of interval is same stable period and 1% is a different-valued stable period, the whole
            # interval will be regarded as unstable (value will stay at NaN in df_feature)
            # N.B. evaluate severity of this issue by printing fraction j/len(intervalCount)
            # N.B2. if using a smaller interval size, e.g. 1 hour, the chance of multiple stable periods
            # (of at least 30 minutes) existing in the same interval is relativey small, so this problem diminishes
            j += 1
        else:
            # apparently value of stable period(s) are within 'dist'
            # set the interval value to the mean of these stable periods if total duration is larger than duration_frac
            overlap = []
            dfcls = df_tmp[(df_tmp['intervalFirst'] <= k) & (df_tmp['intervalLast'] >= k)].loc[[tag]].copy()
            for i in groups[0]:
                max_edge = max(df_features.loc[k]['interval_ts'], dfcls.iloc[i]['cluster_start'])
                min_edge = min(df_features.loc[k]['interval_ts'] + timeperiod, dfcls.iloc[i]['cluster_end'])
                overlap.append(int((min_edge - max_edge).value / 1e9 / 60))

            duration_fraction = sum(overlap) / (nhours * 60)
            df_features.loc[k, 'final_stable_duration'] = sum(overlap)
            if duration_fraction >= duration_frac_limit:
                # total duration of similar-valued stable period(s) is above threshold
                # set value of df_feature to time-weighted mean of the representative values for these stable period(s)
                df_features.loc[k, 'interval_value'] = sum(
                    [elements[e] * overlap[eindex] for eindex, e in enumerate(groups[0])]) / \
                                                       sum(overlap)
            else:
                duration_count += 1

    df_features['interval_ts'] = (df_features['interval_ts'].astype(int) / 1e9).astype(int)

    list_out = list(map(tuple, df_features.itertuples(index=False)))

    return list_out


def identify_clusters_pd(group, columns, dbscan_param, variations):
    """Identify clusters (of stable periods) in timeseries data

    :param ResultIterable group: 3-D tuples with 'interval_ts' (timestamp), 'interval_value' and 'tag_id'
    :param list columns: list of columns labels for the values in 'group'
    :param dict dbscan_param: parameters for clustering algorithm and subsequent filtering
    :param pandas_series variations: Pandas series with typical variations for a tag
    :return: list_out: list of tuples with identified clusters and their typical (median) value
    """

    # reshape input
    list_data = list(zip(*list(group[1])))

    # extract tag_id from first row of list (grouping by tag, so tag_id is the same for each row in the group)
    tag = group[0][0]
    part_win = group[0][1]

    # construct pandas dataframe
    # N.B. this works, but can be replaced by something more direct
    pdf = pd.DataFrame(data={columns[1]: list_data[1]}, index=pd.to_datetime(np.array(list_data[0]) * 1e9),
                       columns=[columns[1]])
    pdf.index.rename(columns[0], inplace=True)
    pdf.sort_index(inplace=True)
    pdf.reset_index(inplace=True)

    # define interval windows
    pdf['interval_win'] = ((pdf['interval_ts'] - pdf['interval_ts'].min()) / (dbscan_param['interval_days'])).dt.days

    # convert required parameters to pd.Timedelta's
    dbscan_param['min_length'] = pd.Timedelta('{0:f} days'.format(dbscan_param['interval_days'] / 2))
    dbscan_param['timegap_subcluster'] = pd.Timedelta(dbscan_param['timegap_subcluster'], unit='m')
    dbscan_param['min_cluster_length'] = pd.Timedelta(dbscan_param['min_cluster_length'], unit='m')

    # parameters that depend on the tag are the scaled eps and spread_cluster parameters and min_points
    # N.B. set minimum of 1e-5 for eps and spread, sometimes the variation is 0 for parameters
    if tag in variations.index:
        typical_var = variations.loc[tag]
    else:
        # set a value for tags for which no typical variation was determined
        # N.B. this is probably beceause there is no variation in such tags 
        typical_var = 1e-8

    # make scale-parameters dependent on typical (daily) variation in tag
    dbscan_param['eps_scaled'] = max(dbscan_param['scale_eps'] * typical_var, 1e-7)
    dbscan_param['spread_cluster_scaled'] = max(dbscan_param['scale_spread'] * typical_var, 1e-7)

    # create a dict for each interval (in which clusters are sought)
    intervals = dict()
    for i in set(pdf['interval_win']):
        interval = pdf[pdf['interval_win'] == i].set_index('interval_ts')[columns[1]].dropna()

        # set min_points based on number of data points for a tag
        dbscan_param['min_samples'] = np.ceil(dbscan_param['min_samples_fraction'] * interval.count())

        timeIndex = interval.index

        if timeIndex.max() - timeIndex.min() >= dbscan_param['min_length']:
            X = interval.values.reshape(-1, 1)

            if dbscan_param['preprocess'] == 'standardise':
                scaler = StandardScaler()
                X = scaler.fit_transform(X)  # standardise

                # also preprocess epsilon and spread_cluster in same way by normalizing with estimated variance
                dbscan_param['eps'] = dbscan_param['eps_scaled'] / np.sqrt(scaler.var_)
                dbscan_param['spread_cluster'] = dbscan_param['spread_cluster_scaled'] / np.sqrt(scaler.var_)
            else:
                dbscan_param['eps'] = dbscan_param['eps_scaled']
                dbscan_param['spread_cluster'] = dbscan_param['spread_cluster_scaled']

            intervals[str(timeIndex.min())] = perform_dbscan(X, timeIndex, dbscan_param)
        else:
            intervals[str(timeIndex.min())] = []

    # create dataframe with relevant features of identified clusters
    # N.B. this works but unnecessarily indirect (relic from nested dict-structure created earlier in Jupyter-notebooks)
    tmp_list_clusters = []
    cluster_keys = ['cluster_start', 'spread_cluster', 'cluster_length_minutes', 'median_val']
    for tstart in intervals:
        cluster_data = intervals[tstart]
        for k in cluster_data:
            if ~np.isnan(cluster_data[k]['spread_cluster']):
                tmp_list = [tag, part_win] + [cluster_data[k][key] for key in cluster_keys]
                tmp_list_clusters.append(tmp_list)

    # reshape output
    list_out = list(map(tuple, tmp_list_clusters))

    return list_out


def perform_dbscan(X, timeIndex, dbscan_param):
    """Apply DBSCAN-clustering algorithm to 1-dimensional data

    DBSCAN makes distinction between core and edge samples in a cluster.
    Here, this distinction is irrelevant, only the cluster label matters.

    :param array X: data in which to identify clusters
    :param pd.Timestamp timeIndex: timeseries-index for samples in X
    :param dict dbscan_param: dictionary with necessary parameters
    :return: dict filter_labels_all: dictionary with clusters that pass filters
    """

    db = DBSCAN(eps=dbscan_param['eps'], min_samples=dbscan_param['min_samples']).fit(X)
    labels = db.labels_

    filter_labels = filter_dbscan_clusters(labels, X, timeIndex, dbscan_param)

    return filter_labels


def filter_dbscan_clusters(labels, X, timeIndex, dbscan_param):
    """Filter identified clusters on duration and spread in values

    :param list labels: label of the identified cluster for each element in X
    :param array X: data in which clusters were identified
    :param pd.Timestamp timeIndex: timeseries-index for samples in X
    :param dict dbscan_param: dictionary with necessary parameters
    :return: dict filter_labels_all: dictionary with clusters that pass filters
    """

    filter_labels_all = dict()
    unique_labels = set(labels)
    j = 0
    for k in unique_labels:
        if k != -1:  # only consider clusters, not outliers (for which k = -1)
            class_member_mask = (labels == k)

            xy = X[class_member_mask]
            xt = timeIndex[class_member_mask]

            xt_diff = xt.to_series().diff() > dbscan_param['timegap_subcluster']
            xt_diff = xt_diff.apply(lambda x: 1 if x else 0).cumsum()

            for i in set(xt_diff):
                j += 1
                xt_interval = xt_diff[xt_diff == i]
                xy_interval = xy[xt_diff == i]

                cluster_start = int(xt_interval.index.min().value / 1e9)
                spread_cluster = xy_interval.max() - xy_interval.min()
                cluster_length_minutes = int((xt_interval.index.max() - xt_interval.index.min()).seconds / 60)
                median_val = np.nanmedian(xy_interval)

                # only keep intervals that are longer than min_length_cluster
                if (dbscan_param['do_minLenCluster']) & \
                        (xt_interval.index.max() - xt_interval.index.min() < dbscan_param['min_cluster_length']):
                    cluster_length_minutes = spread_cluster = median_val = np.nan

                # only keep intervals that are smaller than spread_cluster
                if (dbscan_param['do_spreadCluster']) & (spread_cluster > dbscan_param['spread_cluster_scaled']):
                    cluster_length_minutes = spread_cluster = median_val = np.nan

                filter_labels = {'cluster_index': k, 'subcluster_index': i,
                                 'cluster_start': cluster_start,
                                 'spread_cluster': spread_cluster,
                                 'cluster_length_minutes': cluster_length_minutes,
                                 'median_val': median_val}

                filter_labels_all[j] = filter_labels
    return filter_labels_all


def rolling_median_pd(group, columns, window_width=1, window_points=1):
    """Compute rolling median for window of given width

    :param ResultIterable group: 2-D tuples with 'interval_ts' (timestamp) and 'interval_value' for timestamps in group
    :param list columns: list of columns labels for the values in 'group'
    :param window_width: width of window to compute rolling median for (in minutes)
    :param window_points: minimum number of data points required to compute rolling median
    :return: list list_out: list of 2-D tuples with timestamp and rolling-median for timestamps in group
    """

    # reshape input
    list_data = list(zip(*list(group)))

    # construct pandas dataframe
    df = pd.DataFrame(data={columns[1]: list_data[1]}, index=pd.to_datetime(np.array(list_data[0]) * 1e9),
                      columns=[columns[1]])
    df.sort_index(inplace=True)

    # compute rolling median for pandas dataframe
    df_stable = df.rolling('{0:d}min'.format(window_width),
                           center=False, min_periods=window_points,
                           closed='both').median().shift(-int(window_width / 2))
    df_stable.index = (df_stable.index.astype(int) / 1e9).astype(int)

    # reshape output
    list_out = list(map(tuple, df_stable.itertuples()))

    return list_out


def int_contr_mapper(row, interval_width):
    """Compute contribution of an event pair to a time interval

    :param pyspark.sql.DataFrame row: row with event time, value and time to next event for a tag
    :param int interval_width: width of time interval
    :return: list with contributions for event pair to time interval
    """

    # get boundaries of event time window
    start_ts = row['event_ts'].timestamp()
    end_ts = start_ts + row['time_window']
    int_low = int(start_ts // interval_width)
    int_up = int(np.ceil(end_ts / interval_width))

    # compute boundaries of time intervals
    bounds = np.arange(start=int_low, stop=int_up, step=1)
    bounds *= interval_width
    widths = np.ones(len(bounds)) * interval_width
    widths[0] -= start_ts - bounds[0]
    widths[-1] -= bounds[-1] + interval_width - end_ts

    # return (tag ID, interval timestamp, contribution value, contribution weight, event count) rows
    contr_pos = np.concatenate(([0.], bounds[1:] - start_ts)) + 0.5 * widths

    return list(zip([row['tag_id']] * len(bounds),
                    [row['part_win']] * len(bounds),
                    map(float, bounds + 0.5 * interval_width),
                    map(float, contr_pos / row['time_window'] * row['value_change'] + row['value']),
                    map(float, widths),
                    [1] + [0] * (len(bounds) - 1)))


def build_variation_features(group):
    """Build features based on variations in data

    :param ResultIterable group: tuple with interval timestamps and values for a tag and feature-timestamp
    :return: tuple with features calculated for group
    """

    # get (tag_id, feat_ts) key
    tag_id, feat_ts = group[0]

    # create series of (interval_ts, interval_value) points
    points_lists = tuple(zip(*group[1][0]))
    points = pd.Series(data=points_lists[1], index=points_lists[0]).sort_index()
    points = points.rolling('10min', center=False, min_periods=5, closed='both').median().shift(-5)

    # create series of (interval_ts, interval_value) stable values
    stable_lists = tuple(zip(*group[1][1]))
    stable = pd.Series(data=stable_lists[1], index=stable_lists[0]).sort_index()

    # combine points and stable values
    data = pd.DataFrame(data={'point_value': points, 'stable_value': stable}, columns=['point_value', 'stable_value'])
    data['point_dev'] = data['point_value'] - data['stable_value']

    # construct features
    stable_count = int(data['stable_value'].count())
    dev_count = int(data['point_dev'].count())
    stable_min = float(data['stable_value'].min())
    stable_max = float(data['stable_value'].max())
    stable_mean = float(data['stable_value'].mean())
    stable_median = float(data['stable_value'].median())
    stable_std = float(data['stable_value'].std())
    stable_mad = float((data['stable_value'] - stable_median).abs().median())
    dev_min = float(data['point_dev'].min())
    dev_max = float(data['point_dev'].max())
    dev_rms = float(np.sqrt((data['point_dev'] ** 2).mean()))
    dev_mav = float(data['point_dev'].abs().median())

    return (tag_id, feat_ts, stable_count, dev_count,
            stable_min, stable_max, stable_mean, stable_median, stable_std, stable_mad,
            dev_min, dev_max, dev_rms, dev_mav)


def set_plot_properties(x_range=None, y_range=None, title=None, x_title=None, y_title=None, legend=None, grid=True):
    """Set properties of an existing Matplotlib plot"""

    if grid:
        plt.grid()
    if x_range:
        plt.xlim(*x_range)
    if y_range:
        plt.ylim(*y_range)
    if title:
        plt.title(title, fontsize=22)
    if x_title:
        plt.xlabel(x_title, fontsize=20)
    if y_title:
        plt.ylabel(y_title, fontsize=20)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)

    if legend is not None:
        leg = plt.legend(fontsize=16)
        if leg and not legend:
            leg.remove()


def compute_correlations(df, var1, var2, max_time_shift=0, min_ts=None, max_ts=None, var1_min=None, var1_max=None,
                         var2_min=None, var2_max=None, var_col='tag_id', ts_col='interval_ts',
                         val_col='interval_value'):
    """Compute time-shifted correlation between two variables

    :param pyspark.sql.DataFrame df: data frame with "variable", time stamp", "value" columns
    :param str var1: name of first variable
    :param str var2: name of second variable
    :param int|float max_time_shift: maximum shift in time between variables (seconds)
    :param str min_ts: minimum timestamp of events selected from data frame
    :param str max_ts: maximum timestamp of events selected from data frame
    :param float var1_min: minimum value of first variable
    :param float var1_max: maximum value of first variable
    :param float var2_min: minimum value of second variable
    :param float var2_max: maximum value of second variable
    :param str var_col: key of variable-name column in data frame
    :param str ts_col: key of timestamp column in data frame
    :param str val_col: key of variable-value column in data frame
    :return: pyspark.sql.DataFrame dfts: dataframe with computed time-shifted correlation
    """

    # create variable data frames
    var_dfs = (df.filter(df[var_col] == v) for v in (var1, var2))
    if min_ts:
        var_dfs = (d.filter(d[ts_col] >= min_ts) for d in var_dfs)
    if max_ts:
        var_dfs = (d.filter(d[ts_col] < max_ts) for d in var_dfs)
    var_dfs = (d.filter(d[val_col] >= m) if m else d for d, m in zip(var_dfs, (var1_min, var2_min)))
    var_dfs = (d.filter(d[val_col] < m) if m else d for d, m in zip(var_dfs, (var1_max, var2_max)))
    var_dfs = tuple(d.select(d[ts_col].cast('bigint').alias('ts{:d}'.format(i)),
                             d[val_col].alias('val{:d}'.format(i)))
                    .dropna(how='any', thresh=None) for i, d in enumerate(var_dfs))

    # create data frame of value combinations
    time_shift = sql_funcs.abs(var_dfs[0]['ts0'] - var_dfs[1]['ts1'])
    dfc = var_dfs[0].join(var_dfs[1], how='inner', on=time_shift <= max_time_shift)

    # compute correlation for each time shift
    dfts = dfc.groupBy((dfc['ts0'] - dfc['ts1']).alias('time_shift')).agg(sql_funcs.corr('val0', 'val1').alias('corr'),
                                                                          sql_funcs.count('*').alias('num_samples'))

    return dfts


def normalize_df(df):
    df_rank = (df.rank() - 0.5) / len(df)
    return df_rank.apply(scipy.stats.norm.ppf, axis=0)


def unnormalize_df(df, df_orig):
    if (df.columns != df_orig.columns).all():
        print('WARNING: renaming columns of df')
        df.columns = df_orig.columns
    return df.apply(lambda c: np.percentile(df_orig[c.name], scipy.stats.norm.cdf(c) * 100), axis=0)


def diag_trans(df):
    diag_covs, trans_vects = np.linalg.eig(df.cov())
    return np.diag(1. / np.sqrt(diag_covs)).dot(trans_vects.transpose())


def transform_df(df, trans):
    return pd.DataFrame(df.as_matrix().dot(trans.transpose()))


def inv_transform_df(df, trans):
    normal = pd.DataFrame((np.linalg.pinv(trans).dot(df.as_matrix().T)).transpose())
    # if column_names is not None:
    #    normal.columns = column_names
    return normal


def gen_normal(diag_trans, num_events=1000, column_names=None):
    diag = np.array([np.random.normal(0., 1., num_events) for it in range(len(diag_trans))])
    normal = pd.DataFrame((np.linalg.pinv(diag_trans).dot(diag)).transpose())
    if column_names is not None:
        normal.columns = column_names
    return normal


def flag_time_interval(flag1, flag2):
    """Flag time interval as stable/unstable based on both backward and forward buffer

    Possible outcomes for output flag:
    0 = unstable: unstable in both buffers, or unstable in one buffer and undetermined in other buffer
    1 = stable: stable in both buffers, or stable in one buffer and undetermined in other buffer
    2 = undetermined: undetermined in both buffers
    3 = stable in one buffer, unstable in other buffer

    :param int flag1: flag in backward buffer
    :param int flag2: flag in forward buffer
    :return int flag: flag indicating stability of time interval (can be 0, 1, 2 or 3)
    """

    if (flag1 == 1 and flag2 == 0) or (flag1 == 0 and flag2 == 1):
        flag = 3
    else:
        flag = min(flag1, flag2)

    return flag
