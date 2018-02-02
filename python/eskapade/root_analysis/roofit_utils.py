"""Project: Eskapade - A Python-based package for data analysis.

Module: root_analysis.roofit_utils

Created: 2017/04/24

Description:
    Basic utilities for interaction with RooFit

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pathlib
from enum import IntEnum, unique

import ROOT
from ROOT import RooFit

import sys

from eskapade.logger import LogLevel, Logger
from eskapade import resources

CUSTOM_ROOFIT_OBJECTS = ('RooComplementCoef',
                         'RooNonCentralBinning',
                         'RooTruncExponential',
                         'RooWeibull',
                         ('Eskapade', 'PlotCorrelationMatrix'))


@unique
class RooFitLogLevel(IntEnum):
    """RooFit logging level integer enumeration class.

    The enumerations are:

    * DEBUG    (0 == eskapade.logger.LogLevel.NOTSET)
    * INF0     (1 == eskapade.logger.LogLevel.DEBUG / 10)
    * PROGRESS (2 == eskapade.logger.LogLevel.INFO / 10)
    * WARNING  (3 == eskapade.logger.LogLevel.WARNING / 10)
    * ERROR    (4 == eskapade.logger.LogLevel.ERROR / 10)
    * FATAL    (5 == eskapade.logger.LogLevel.FATAL / 10)

    They have the same meaning and value as the RooFit MsgLevel Enumerations.
    """

    DEBUG = LogLevel.NOTSET
    INFO = LogLevel.DEBUG / 10
    PROGRESS = LogLevel.INFO / 10
    WARNING = LogLevel.WARNING / 10
    ERROR = LogLevel.ERROR / 10
    FATAL = LogLevel.FATAL / 10

    def __str__(self) -> str:
        return self.name


ROO_INF = ROOT.RooNumber.infinity()

logger = Logger()


def set_rf_log_level(level):
    """Set RooFit log level."""
    if level not in RooFitLogLevel:
        return
    ROOT.RooMsgService.instance().setGlobalKillBelow(level)


def load_libesroofit():
    """Load Eskapade RooFit library."""

    # the Eskapade RooFit library name
    esroofit_lib_base = 'libesroofit'
    esroofit_lib_ext = '.so'  # Default is nix.
    if sys.platform == 'darwin':
        esroofit_lib_ext = '.dylib'

    esroofit_lib_name = esroofit_lib_base + esroofit_lib_ext

    # don't rebuild/reload library if already loaded
    if any(esroofit_lib_base in _ for _ in ROOT.gSystem.GetLibraries().split()):
        return

    logger.debug('(Re-)loading Eskapade RooFit library')

    path_to_esroofit = resources.lib(esroofit_lib_name)
    # The ROOT/Cling dictionary needs the esroofit header files.
    # These are located relative to the libesroofit library.
    esroofit_parent = str(pathlib.PurePath(path_to_esroofit).parent)
    # We need to tell the ROOT interpreter where to search for
    # the esroofit headers.
    root_interpreter = ROOT.gROOT.GetInterpreter()
    root_interpreter.AddIncludePath(esroofit_parent)

    # load library
    if ROOT.gSystem.Load(path_to_esroofit) != 0:
        raise RuntimeError('Failed to load Eskapade RooFit library!')

    # check if all classes are loaded
    try:
        for obj_spec in CUSTOM_ROOFIT_OBJECTS:
            if isinstance(obj_spec, str):
                obj_spec = obj_spec.split('.')
            obj = ROOT
            for spec_comp in obj_spec:
                obj = getattr(obj, spec_comp)
    except AttributeError as e:
        raise RuntimeError('Could not load all custom RooFit objects: "{exception!s}"'.format(exception=e))


_roo_cmd_args = []


def create_roofit_opts(opts_type='', create_linked_list=True, **kwargs):
    """Build list of options for RooFit functions.

    Additional keyword arguments are appended to default options list for
    specified type.  The keys must be names of functions in the RooFit
    namespace that return a RooCmdArg, while the values are the arguments of
    the specified function:

    >>> opts = create_roofit_opts('data_plot', LineColor=ROOT.kRed, Asymmetry=cat, Binning=(10, 0., 10.))
    >>> data.plotOn(frame, opts)

    :param str opts_type: options type {'', 'fit', 'pdf_plot', 'data_plot', 'obs_frame'}
    :returns: collection of RooFit options
    :rtype: dict or ROOT.RooLinkedList
    """
    # check option type
    if opts_type not in [None, '', 'fit', 'pdf_plot', 'data_plot', 'obs_frame']:
        raise RuntimeError('unknown options type: "{}"'.format(opts_type))

    # create default list for specified type
    opts = {}
    if opts_type == 'fit':
        opts.update(dict(Minimizer='Minuit2', Strategy=1, Optimize=2, NumCPU=1, Offset=True, Save=True,
                         PrintLevel=1, Timer=True))
    elif opts_type == 'pdf_plot':
        opts.update(dict(LineColor=ROOT.kBlue, LineWidth=2))
    elif opts_type == 'data_plot':
        opts.update(dict(MarkerStyle=ROOT.kFullDotLarge, MarkerColor=ROOT.kBlack, MarkerSize=0.5,
                         LineColor=ROOT.kBlack, LineWidth=1))

    # update list with specified options
    opts.update(kwargs)

    # create options
    ret_opts = {}
    for opt_fun, val in opts.items():
        # parse options-function key
        opt_fun = ''.join(s[0].upper() + s[1:] for s in opt_fun.split('_'))
        if not hasattr(RooFit, opt_fun) or not callable(getattr(RooFit, opt_fun)):
            raise RuntimeError('no command-arg function "{}" in RooFit'.format(opt_fun))

        # create RooCmdArg with function "opt_fun" and value "val"
        cmd_arg = getattr(RooFit, opt_fun)(*((val,) if not any(isinstance(val, t) for t in (tuple, list)) else val))
        ret_opts[opt_fun] = cmd_arg
        _roo_cmd_args.append(cmd_arg)  # keeps command args alive

    if create_linked_list:
        # return options in a RooLinkedList
        ret_opts_list = ROOT.RooLinkedList()
        for cmd_arg in ret_opts.values():
            ret_opts_list.Add(cmd_arg)
        return ret_opts_list

    return ret_opts
