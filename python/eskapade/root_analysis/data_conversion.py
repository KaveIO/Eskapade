"""Project: Eskapade - A Python-based package for data analysis.

Module: root_analysis.data_conversion

Created: 2017/04/24

Description:
    Converters between ROOT, RooFit, NumPy, and Pandas data formats

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import collections
import uuid

import ROOT
import numpy as np
import pandas as pd
from root_numpy import array2tree, tree2array

from eskapade.analysis.histogram import Histogram


def to_root_hist(histogram, **kwargs):
    """Convert Eskapade histogram to root histogram.

    Input Eskapade histogram first gets converted to a numpy histogram,
    which is then converted to a root histogram.  All kwargs besides the
    input histograms are passed on to histogram.get_bin_vals(), which makes
    the numpy histogram.

    :param histogram: input Eskapade histogram
    :returns: root histogram
    :rtype: ROOT.TH1
    """
    if not isinstance(histogram, Histogram):
        raise TypeError('histogram not of type Histogram')
    # convert to ROOT histogram
    new_var_name = str(kwargs.pop('variable', histogram.variable))
    return bin_vals_to_hist(histogram.get_bin_vals(**kwargs), var_name=new_var_name)


def bin_vals_to_hist(vals, var_name='x'):
    """Convert numpy to root histogram.

    :param vals: two comma-separated arrays: bin_entries, bin_edges, or bin_entries, bin_labels,
                 representing a 1-dimensional histogram as returned by numpy.histogram()
    :returns: 1-dimensional root histogram
    :rtype: ROOT.TH1
    """
    # get histogram bin values
    bin_entries, bins = vals
    if len(bin_entries) == len(bins):
        bin_edges = np.linspace(0., float(len(bins)), num=len(bins) + 1)
        bin_labs = [str(b) for b in bins]
    elif len(bin_entries) == len(bins) - 1:
        bin_edges = np.array(bins)
        bin_labs = []
    else:
        raise AssertionError('number of bin values does not correspond to number of bins')

    # create root histogram
    hist = ROOT.TH1D(uuid.uuid4().hex, '', len(bin_edges) - 1, bin_edges)
    for it, lab in enumerate(bin_labs):
        # set bin labels
        hist.GetXaxis().SetBinLabel(it + 1, lab)
    for it, val in enumerate(bin_entries):
        # set bin entries
        hist.SetBinContent(it + 1, val)
        hist.SetBinError(it + 1, np.sqrt(val))
    hist.SetEntries(np.sum(bin_entries))

    # set some root histogram properties
    hist.SetMinimum(0.)
    hist.SetXTitle(var_name)
    hist.SetYTitle('Number of bin entries')

    return hist


def hist_to_bin_vals(hist):
    """Convert root histogram to numpy bin_vals.

    Create bin_counts and bin_edges lists, similar to np.histogram()
    function.

    :param ROOT.TH1 hist: input root histogram, assumed to be 1-dimensional.
    :returns: two comma-separated arrays: bin_entries, bin_edges
    """
    # check input type
    assert isinstance(hist, ROOT.TH1), 'root hist needs to be 1-dimensional'

    # create bin_counts and bin_edges lists, similar to np.histogram() function
    bin_entries = []
    bin_edges = []
    n_bins = hist.GetNbinsX()
    for i in range(n_bins):
        bin_entries.append(hist.GetBinContent(i + 1))
        bin_edges.append(hist.GetBinLowEdge(i + 1))
    bin_edges.append(hist.GetBinLowEdge(n_bins + 1))

    return bin_entries, bin_edges


def hist_to_rdh(hist, obs='x'):
    """Convert root histogram to roodatahist object.

    Convert root histogram to corresponding roodatahist object, which can be
    used as dataset by roofit.

    :param ROOT.TH1 hist: input root histogram. Can be 1,2 or 3 dimensional.
    :param str obs: comma-separated string of observable names in the histogram. E.g. 'x' or 'x,y'.
                    Needs to match the number of input dimensions.
    :returns: comma-separated roodatahist and rooarglist, the latter
              representing the corresponding observables in roofit.
    :rtype: RooDataHist, RooArgList
    """
    # check input histogram
    if not issubclass(type(hist), ROOT.TH1):
        raise TypeError('root hist needs to inherit from TH1')

    if isinstance(hist, ROOT.TH3):
        n_dim = 3
    elif isinstance(hist, ROOT.TH2):
        n_dim = 2
    elif isinstance(hist, ROOT.TH1):
        n_dim = 1
    else:
        raise RuntimeError('cannot assess number of histogram dimensions.')

    # retrieve min, max, n_bins properties of axes
    n_bins = []
    min_max = []
    for d in range(n_dim):
        if d == 0:
            axis = hist.GetXaxis()
        elif d == 1:
            axis = hist.GetYaxis()
        elif d == 2:
            axis = hist.GetZaxis()
        n_bins.append(axis.GetNbins())
        min_max.append((axis.GetXmin(), axis.GetXmax()))

    # create corresponding list of roofit observables
    obs_list = ROOT.RooArgList('obs_list_' + hist.GetName())
    # python does not take ownership of the list
    ROOT.SetOwnership(obs_list, False)
    if isinstance(obs, str):
        assert len(obs), 'obs needs to be a colon-separated filled string (eg. x:y:z) or list'
        obs_arr = obs.split(':')
        assert n_dim == len(obs_arr), 'obs needs to be a colon-separated filled string (eg. x:y:z) or list'
        obs = obs_arr
    if isinstance(obs, list):
        assert n_dim == len(obs), 'obs needs to be a colon-separated filled string (eg. x:y:z) or list'
        for i, o in enumerate(obs):
            rrv = ROOT.RooRealVar(o, o, min_max[i][0], min_max[i][1])
            # python does not take ownership of rrv
            ROOT.SetOwnership(rrv, False)
            obs_list.addOwned(rrv)
    elif isinstance(obs, ROOT.RooRealVar) and n_dim == 1:
        obs_list.add(obs)
    elif isinstance(obs, ROOT.RooArgList):
        assert len(obs) == n_dim, 'obs needs to be a RooRealVar or RooArgList of correct length'
        obs_list.add(obs)
    else:
        raise TypeError('cannot determine observables of histogram.')

    # fill roodatahist object
    rdh = ROOT.RooDataHist('rdh_' + hist.GetName(), hist.GetTitle(), obs_list, hist)
    ROOT.SetOwnership(rdh, False)

    # contents check
    n_rdh = rdh.sumEntries()
    n_hist = hist.GetSumOfWeights()
    if n_rdh != n_hist:
        raise AssertionError('records have been lost: rdh "{0:d}" vs hist "{1:d}"'.format(n_rdh, n_hist))

    return rdh, obs_list


def hist_to_rds(hist, obs='x'):
    """Convert root histogram to roodataset object.

    Convert root histogram to corresponding roodataset object, which can be
    used as dataset by roofit.

    :param ROOT.TH1 hist: input root histogram. Can be 1,2 or 3 dimensional.
    :param str obs: comma-separated string of observable names in the histogram. E.g. 'x' or 'x,y'.
                    Needs to match the number of input dimensions.
    :returns: comma-separated roodataset and rooargset, the latter
              representing the corresponding observables in roofit.
    :rtype: RooDataHist, RooArgList
    """
    # check input histogram
    if not issubclass(type(hist), ROOT.TH1):
        raise TypeError('root hist needs to inherit from TH1')

    if isinstance(hist, ROOT.TH3):
        n_dim = 3
    elif isinstance(hist, ROOT.TH2):
        n_dim = 2
    elif isinstance(hist, ROOT.TH1):
        n_dim = 1
    else:
        raise RuntimeError('cannot assess number of histogram dimensions')

    # retrieve min, max, n_bins properties of axes
    n_bins = []
    min_max = []
    for d in range(n_dim):
        if d == 0:
            axis = hist.GetXaxis()
        elif d == 1:
            axis = hist.GetYaxis()
        elif d == 2:
            axis = hist.GetZaxis()
        n_bins.append(axis.GetNbins())
        min_max.append((axis.GetXmin(), axis.GetXmax()))

    # create corresponding list of roofit observables
    obs_set = ROOT.RooArgSet('obs_set_' + hist.GetName())
    obsw_set = ROOT.RooArgSet('obsw_set_' + hist.GetName())
    # note: python does not take ownership of the list
    ROOT.SetOwnership(obs_set, False)
    ROOT.SetOwnership(obsw_set, False)
    if isinstance(obs, str):
        assert len(obs), 'obs needs to be a colon-separated filled string (eg. x:y:z) or list'
        obs_arr = obs.split(':')
        assert n_dim == len(obs_arr), 'obs needs to be a colon-separated filled string (eg. x:y:z) or list'
        obs = obs_arr
    if isinstance(obs, list):
        assert n_dim == len(obs), 'obs needs to be a colon-separated filled string (eg. x:y:z) or list'
        for i, o in enumerate(obs):
            rrv = ROOT.RooRealVar(o, o, min_max[i][0], min_max[i][1])
            # note: python does not take ownership of rrv
            ROOT.SetOwnership(rrv, False)
            obs_set.addOwned(rrv)
            obsw_set.addOwned(rrv)
    elif isinstance(obs, ROOT.RooRealVar) and n_dim == 1:
        obs_set.add(obs)
        obsw_set.add(rrv)
    elif isinstance(obs, ROOT.RooArgSet):
        assert len(obs) == n_dim, 'obs needs to be a RooRealVar or RooArgSet of right length'
        obs_set.add(obs)
        obsw_set.add(obs)
    else:
        raise TypeError('cannot determine observables of histogram')

    # roodataset filled with a histogram needs a weight variable
    weight = ROOT.RooRealVar('weight', 'weight', 0)
    ROOT.SetOwnership(weight, False)
    obsw_set.addOwned(weight)

    # create roodataset, make sure it can be easily converted to a tree later on.
    ROOT.RooAbsData.setDefaultStorageType(ROOT.RooAbsData.Tree)
    rds = ROOT.RooDataSet('rds_' + hist.GetName(), hist.GetTitle(), obsw_set, weight.GetName())
    ROOT.SetOwnership(rds, False)

    # start filling the roodataset ...

    # array for easily setting roorealvar values
    values = []
    for rrv in obs_set:
        values.append(rrv)

    # loop over upto 3 dimensions of the histogram
    ax = hist.GetXaxis()
    ay = hist.GetYaxis()
    az = hist.GetZaxis()
    for i in range(ax.GetNbins()):
        xval = ax.GetBinCenter(i + 1)
        values[0].setVal(xval)
        if n_dim == 1:
            fval = hist.GetBinContent(i + 1)
            if fval == 0:
                continue
            weight.setVal(fval)
            rds.add(obsw_set, fval)
        else:  # 2 or more dims
            for j in range(ay.GetNbins()):
                yval = ay.GetBinCenter(j + 1)
                values[1].setVal(yval)
                if n_dim == 2:
                    fval = hist.GetBinContent(i + 1, j + 1)
                    if fval == 0:
                        continue
                    weight.setVal(fval)
                    rds.add(obsw_set, fval)
                else:  # 3 dims
                    for k in range(az.GetNbins()):
                        fval = hist.GetBinContent(i + 1, j + 1, k + 1)
                        if fval == 0:
                            continue
                        zval = az.GetBinCenter(k + 1)
                        values[2].setVal(zval)
                        weight.setVal(fval)
                        rds.add(obsw_set, fval)
    return rds, obs_set


def df_to_tree(df, name='tree', tree=None, store_index=True):
    """Convert DataFrame to a ROOT TTree.

    Inspired by to_root() by root_pandas:
    https://github.com/ibab/root_pandas/blob/master/root_pandas/readwrite.py#L242

    Modified to convert boolean and categorical observables.

    :param pandas.DataFrame df: input pandas dataframe, to be converted to a TTree
    :param str name: Name of the created ROOT TTree if ``tree`` is None. optional, default='tree'.
    :param TTree tree: An existing ROOT TTree to be extended by the numpy array.
                       Any branch with the same name as a field in the numpy array will be extended as
                       long as the types are compatible, otherwise a TypeError is raised. New
                       branches will be created and filled for all new fields.
                       optional, default=None.
    :param bool store_index: store the index as a separate tree branch. Default is true.
                             The DataFrame index will be saved as a branch called '__index__*',
                             where * is the name of the index in the original DataFrame
    :returns: comma-separated tree and dict with conversion maps of boolean and categorical observables.
    :rtype: TTree, dict
    """
    if df is None:
        return None, {}

    # handle single columns
    if isinstance(df, pd.core.series.Series):
        df = pd.DataFrame(df)

    # We don't want to modify the user's DataFrame here, so we make a shallow copy
    df_ = df.copy(deep=False)

    # convert unsupported datatypes (by roodataset)
    map_to_factorized = {}
    for col in df_.columns:
        # 1. convert categorical data and boolians to integers
        dt = df_[col].dtype
        if pd.core.common.is_categorical(dt) or dt == 'bool':
            labels, unique = df_[col].factorize()
            df_[col] = labels
            # store the mapping for possible use in roocategories
            map_to_factorized[col] = dict((v, i) for i, v in enumerate(unique))
        # 2. roofit cannot handle int64 (long_t), so convert to int32
        dt = df_[col].dtype
        if isinstance(dt.type(), np.int64):
            df_[col] = df_[col].astype(np.int32)

    # keep df's index if requested so
    if store_index:
        idx_name = df_.index.name
        if idx_name is None:
            # Handle the case where the index has no name
            idx_name = ''
        df_['__index__' + idx_name] = np.int32(df_.index)

    # do conversion to ttree
    arr = df_.to_records(index=False)
    tree = array2tree(arr, name, tree)

    # basic check of contents
    n_tree = tree.GetEntries()
    n_df = len(df_.index)
    if n_tree != n_df:
        raise AssertionError('records have been lost: tree "{0:d}" vs df "{1:d}"'.format(n_tree, n_df))

    return tree, map_to_factorized


def series_to_tree(series, name='tree', tree=None):
    """Convert Series to a ROOT TTree.

    :param pandas.Series series: input pandas series object, to be converted to a TTree
    :param str name: Name of the created ROOT TTree if ``tree`` is None. optional, default='tree'.
    :param TTree tree: An existing ROOT TTree to be extended by the numpy array.
                       Any branch with the same name as a field in the numpy array will be extended as
                       long as the types are compatible, otherwise a TypeError is raised. New
                       branches will be created and filled for all new fields.
                       optional, default=None.
    :returns: comma-separated tree and dict with conversion maps of boolean or categorical observable.
    :rtype: TTree, dict
    """
    return df_to_tree(pd.DataFrame(series), name, tree)


def _get_tree_branches(tree):
    """Get all branch names of a tree."""
    branch_list = tree.GetListOfBranches()
    return [branch_list.At(i).GetName() for i in range(branch_list.GetEntries())]


def _get_exist_branches(branch_names, all_branch_names):
    """Filter out not existing branches from branch names or return all branches."""
    if not branch_names:
        return all_branch_names
    return set(branch_names).intersection(all_branch_names)


def tree_to_df(tree, branch_names=None, index_name='', drop_roofit_labels=False):
    """Convert a TTree to a pandas DataFrame.

    :param TTree tree: An existing ROOT TTree to be converted to a pandas DataFrame
    :param list branch_names: input list of branch to be converted dataframe columns.
        If empty, pick all branches. Optional.
    :param str index_name: branch that will be interpreted as dataframe index.
        If empty, pick first branch name starting with '__index__'. Optional.
    :param bool drop_roofit_labels: drop branches ending on '_lbl', as produced by roofit categorical variables.
        For all other variables the postfix '_idx' is removed. Optional.
    :returns: dataframe
    :rtype: pandas.DataFrame
    """
    if tree is None:
        return None

    # 1. list of branches to consider
    all_branch_names = _get_tree_branches(tree)
    branch_names = _get_exist_branches(branch_names, all_branch_names)
    if drop_roofit_labels:
        branch_names = set(filter(lambda bn: not bn.endswith('_lbl'), branch_names))

    # 2. convert to df
    arrs = tree2array(tree, branch_names)
    df = pd.DataFrame(arrs)

    # 3. convert index back to integers
    # try to find index name. make educated guess
    if not index_name:
        for col in df.columns:
            if col.startswith('__index__'):
                index_name = col
                break
    if index_name:
        try:
            df[index_name] = df[index_name].astype(np.int32)
            df.set_index(index_name, inplace=True)
        except BaseException:
            pass

    # 4. convert names of roocategories back to normal column names
    #    needed for conversion back to string based columns later on.
    if drop_roofit_labels:
        df.columns = [col.replace('_idx', '') for col in df.columns]

    # 5. basic check of contents
    n_tree = tree.GetEntries()
    n_df = len(df.index)
    if n_tree != n_df:
        raise AssertionError('records have been lost: tree "{0:d}" vs df "{1:d}"'.format(n_tree, n_df))

    return df


def rds_to_tree(rds, tree_name='', ignore_lost_records=False):
    """Convert a RooDataSet to a TTree.

    :param ROOT.RooDataSet rds: an existing ROOT RooDataSet to be converted to a ROOT TTree
    :param str tree_name: new name of the tree. (optional)
    :param bool ignore_lost_records: if true, tree is allowed to loose records in conversion
    :returns: a root tree
    :rtype: ROOT.TTree
    """
    if rds is None:
        return None

    # direct conversion from rds only works if internal storage is a tree
    storage_type = ROOT.RooAbsData.getDefaultStorageType()
    if storage_type == ROOT.RooAbsData.Tree:
        tree = rds.tree().Clone()
    else:
        # convert to make it so.
        ROOT.RooAbsData.setDefaultStorageType(ROOT.RooAbsData.Tree)
        new_rds = ROOT.RooDataSet(uuid.uuid4().hex, rds.GetTitle(), rds, rds.get())
        tree = new_rds.tree().Clone()
        del new_rds

    # basic check of contents
    if not ignore_lost_records:
        n_tree = tree.GetEntries()
        n_rds = rds.numEntries()
        if n_tree != n_rds:
            raise AssertionError('records have been lost: tree "{0:d}" vs rds "{1:d}"'.format(n_tree, n_rds))

    if len(tree_name):
        tree.SetName(tree_name)
    return tree


def tree_to_rds(tree, rf_varset=None, branch_names=None, name='', category_vars=None):
    """Convert root tree to roodataset object.

    Convert root TTree to corresponding roodataset object, that can be used
    as dataset by roofit.

    :param ROOT.TTree tree: input root tree to be converted
    :param ROOT.RooArgSet rf_varset: if set, pick these (roofit) observables as tree's input branches.
                                     Assumption is that these observables match with the tree.
                                     Default is None. Optional.
    :param list branch_names: input list of branch to be converted to roodataset. If empty, pick all branches.
                              If branches are unknown they are skipped. Optional.
    :param str name: new name of the roodataset. If empty, pick 'rds' + '_' + tree name (optional)
    :param dict category_vars: input dict with known conversion maps of boolean or categorical observable
                               to integer. Default is {}. Optional.
    :returns: comma-separated roodataset, rooargset, and dict with conversion maps of integer back to
              boolean or categorical observable.
    :rtype: RooDataHist, RooArgSet, dict
    """
    if tree is None:
        return None, None, {}

    # 0. list of branches to consider
    all_branch_names = _get_tree_branches(tree)

    # 1. construct the RooDataSet using the existing roofit variables (and their ranges)
    #    Assumption is that these observables match with the tree!
    if rf_varset:
        for rv in rf_varset:
            if rv.GetName() not in all_branch_names:
                raise KeyError('RooFit var "{0:s}" not in input tree "{1:s}"'.format(rv.GetName(), tree.GetName()))
        if not name:
            name = 'rds_' + tree.GetName()
        # we set RooDataSet's internal format to TTrees,
        # so that, potentially, a ttree can be retrieved later on.
        ROOT.RooAbsData.setDefaultStorageType(ROOT.RooAbsData.Tree)
        rds = ROOT.RooDataSet(name, tree.GetTitle(), tree, rf_varset)
        return rds, rf_varset, {}

    # beyond this point no roofit variables have been provided.
    # determine these from the tree directly.

    # 2. determine min and max for all numeric branches
    # this is needed below when reading in the tree in a roodataset,
    # where values outside a variables range are automatically rejectsed.
    var_specs = collections.OrderedDict()
    # branches that become roorealvars

    if category_vars is None:
        category_vars = {}
    branch_names = _get_exist_branches(branch_names, all_branch_names)

    for bn in set(branch_names).difference(category_vars):
        try:
            bmin = tree.GetMinimum(bn)
            bmax = tree.GetMaximum(bn)
            # ensure no under or overflows
            bdiff = abs(bmax - bmin)
            bmax += 0.05 * bdiff
            bmin -= 0.05 * bdiff
            # Get string of datatype with
            dtype_str = tree.GetBranch(bn).GetLeaf(bn).GetTypeName()
            var_specs[bn] = (dtype_str, bmin, bmax)
        except BaseException:
            # roodataset only accepts numeric or boolian observables; skipping non-numeric branches
            pass

    # 3a. construct corresponding roocategories, needed for roodataset
    values = ROOT.RooArgSet()
    map_to_original = {}
    # branches that become roocategories
    for bn in set(branch_names).intersection(category_vars):
        label_dict = category_vars[bn]
        rcat = ROOT.RooCategory(bn, bn)
        for label, val in label_dict.items():
            rcat.defineType(str(label), val)
        # python does not take ownership of rcat
        # https://root-forum.cern.ch/t/crash-when-using-rooargset/21868
        ROOT.SetOwnership(rcat, False)
        values.addOwned(rcat)
        map_to_original[bn] = dict((i, v) for v, i in label_dict.items())

    # 3b. construct corresponding roorealvars, needed for roodataset
    for bn in var_specs:
        var_range = var_specs[bn]
        rrv = ROOT.RooRealVar(bn, bn, var_range[1], var_range[2])
        # python does not take ownership of rrv
        # https://root-forum.cern.ch/t/crash-when-using-rooargset/21868
        ROOT.SetOwnership(rrv, False)
        values.addOwned(rrv)

    # 4. construct the RooDataSet
    # we set RooDataSet's internal format to TTrees,
    # so that, potentially, a ttree can be retrieved later on.
    if len(name) == 0:
        name = 'rds_' + tree.GetName()
    ROOT.RooAbsData.setDefaultStorageType(ROOT.RooAbsData.Tree)
    rds = ROOT.RooDataSet(name, tree.GetTitle(), tree, values)

    # basic conversion check
    n_tree = tree.GetEntries()
    n_rds = rds.numEntries()
    if n_tree != n_rds:
        raise AssertionError('records have been lost: tree "{0:d}" vs rds "{1:d}"'.format(n_tree, n_rds))

    # cleanup: remove index variables from rooargset
    # ... these are just there for bookkeeping purposes
    for bn in var_specs:
        if bn.startswith('__index__'):
            values.remove(values[bn])

    return rds, values, map_to_original


def df_to_rds(df, rf_varset=None, category_vars=None, name='', store_index=True):
    """Convert a pandas dataframe to roodataset object.

    Convert pandas DataFrame to RooDataSet object, that can be used as dataset by roofit.

    :param pandas.DataFrame df: input dataframe to be converted
    :param ROOT.RooArgSet rf_varset: if set, pick these (roofit) observables as tree's input branches.
                                     Assumption is that these observables match with the tree.
                                     Default is None. (optional)
    :param dict category_vars: input dict with known conversion maps of boolean or categorical observable
                               to integer. (optional)
    :param str name: new name of the roodataset. If empty, pick 'rds' + '_' + tree name (optional)
    :returns: comma-separated roodataset, rooargset, dict with updated conversion maps of
              extra boolean or categorical observable to integer, dict with conversion maps
              of integer back to boolean or categorical observable.
    :rtype: RooDataHist, RooArgSet, dict, dict
    """
    if category_vars is None:
        category_vars = {}
    if df is None:
        return None, None

    # 1. convert to tree
    tree, map_to_factorized = df_to_tree(df, name=name, store_index=store_index)
    #    update to complete set of categorical vars
    map_to_factorized.update(category_vars)
    # 2. convert tree to rds
    rds, varset, map_to_original = tree_to_rds(tree, rf_varset=rf_varset, category_vars=map_to_factorized)
    #    cleanup
    del tree
    return rds, varset, map_to_factorized, map_to_original


def rds_to_df(rds, branch_names=None, index_name='', ignore_lost_records=False):
    """Convert a roodataset to a pandas DataFrame.

    :param ROOT.RooDataSet rds: An existing ROOT RooDataSet to be converted to a pandas DataFrame
    :param list branch_names: input list of branch to be converted dataframe columns.
        If empty, pick all branches. Optional.
    :param str index_name: roofit observable that will be interpreted as dataframe index.
        If empty, pick first branch name starting with '__index__'. Optional.
    :param bool ignore_lost_records: if true, dataframe is allowed to loose records in conversion. Optional.
    :returns: dataframe
    :rtype: pandas.DataFrame
    """
    if rds is None:
        return None
    tree = rds_to_tree(rds, ignore_lost_records=ignore_lost_records)
    df = tree_to_df(tree, branch_names=branch_names, index_name=index_name, drop_roofit_labels=True)
    del tree
    return df


def rdh_to_rds(rdh):
    """Convert a roodatahist to a roodataset.

    :param ROOT.RooDataHist rdh: An existing ROOT RooDataHist to be converted to a ROOT.RooDataSet
    :returns: ROOT RooDataSet
    :rtype: ROOT.RooDataSet
    """
    if rdh is None:
        return None

    if not isinstance(rdh, ROOT.RooDataHist):
        raise TypeError('input object not of type RooDataHist, but: {}'.format(type(rdh)))
    assert rdh.numEntries() > 0, 'RooDataHist "{}" is empty.'.format(rdh.GetName())

    # create corresponding list of roofit observables
    obs_set = rdh.get()

    obsw_set = ROOT.RooArgSet('obsw_set')
    num_entries = ROOT.RooRealVar('num_entries', 'num_entries', 0)
    weight = ROOT.RooRealVar('weight', 'weight', 0)
    weight_error = ROOT.RooRealVar('weight_error', 'weight_error', 0)

    # roodataset will get weight variable
    obsw_set.add(obs_set)
    obsw_set.add(num_entries)
    obsw_set.add(weight_error)
    obsw_set.add(weight)

    # create roodataset, make sure it can be easily converted to a tree later on.
    ROOT.RooAbsData.setDefaultStorageType(ROOT.RooAbsData.Tree)
    rds = ROOT.RooDataSet('rds_' + rdh.GetName(), rdh.GetTitle(), obsw_set, weight.GetName())
    ROOT.SetOwnership(rds, False)

    for i in range(rdh.numEntries()):
        rdh.get(i)  # this call updates obs_set
        num_entries.setVal(rdh.weight())
        weight_error.setVal(rdh.weightError())
        weight.setVal(rdh.weight())
        rds.add(obsw_set, rdh.weight())

    return rds


def rds_to_rdh(rds, rf_varset=None, columns=None, binning_name=''):
    """Convert a RooDataSet to a RooDataHist.

    :param ROOT.RooDataSet rds: An existing ROOT RooDataSet to be converted to a pandas DataFrame
    :param ROOT.RooArgSet rf_varset: roofit variables used in RooDataHist constructor
    :param iterable columns: list of columns to create RooDataHist for (alternative to rf_varset)
    :param str binning_name: name of binning configuration, used in RooDataHist constructor
    :returns: RooDataHist of selected columns
    :rtype: ROOT.RooDataHist
    """
    if not isinstance(rds, ROOT.RooDataSet):
        raise AssertionError('input object not of type RooDataSet')
    if rds.numEntries() == 0:
        raise AssertionError('input dataset is not filled')

    if rf_varset is not None:
        if not (isinstance(rf_varset, ROOT.RooArgSet) and len(rf_varset) > 0):
            raise AssertionError('rf_varset is not a filled RooArgSet')
        columns = [arg.GetName() for arg in rf_varset]
    if not columns:
        raise AssertionError('columns list is empty')

    varset = rds.get(0)
    for col in columns:
        if not varset.find(col):
            raise AssertionError('column "{}" not found in input dataset'.format(col))

    if not rf_varset:
        rf_varset = ROOT.RooArgSet()
        for col in columns:
            rf_varset.add(varset.find(col))

    # create and fill roofit histogram
    rdh_name = 'rdh_' + rds.GetName()
    rdh = ROOT.RooDataHist(rdh_name, rdh_name, rf_varset, binning_name)
    rdh.add(rds)

    return rdh
