import logging

import ROOT
from ROOT import RooFit

import eskapade.utils

CUSTOM_ROOFIT_CLASSES = ('RooComplementCoef', 'RooTruncExponential')
ROOFIT_LOG_LEVELS = {'DEBUG': 0, 'INFO': 1, 'WARN': 3, 'WARNING': 3, 'ERROR': 4, 'FATAL': 5, 'CRITICAL': 5, 'OFF': 5,
                     logging.DEBUG: 0, logging.INFO: 1, logging.WARNING: 3, logging.ERROR: 4, logging.CRITICAL: 5,
                     logging.FATAL: 5, logging.OFF: 5}
ROO_INF = ROOT.RooNumber.infinity()

log = logging.getLogger(__name__)


def set_rf_log_level(level):
    """Set RooFit log level"""

    if level not in ROOFIT_LOG_LEVELS:
        return
    ROOT.RooMsgService.instance().setGlobalKillBelow(ROOFIT_LOG_LEVELS[level])


def load_libesroofit():
    """Load Eskapade RooFit library"""

    # don't rebuild/reload library if already loaded
    if any(lib.endswith('/libesroofit.so') for lib in ROOT.gSystem.GetLibraries().split()):
        return
    log.debug('(Re-)building and loading Eskapade RooFit library')

    # (re)build library
    eskapade.utils.build_cxx_library(lib_key='roofit', accept_existing=True)

    # load library
    if ROOT.gSystem.Load('libesroofit') != 0:
        raise RuntimeError('Failed to load Eskapade RooFit library')

    # check if all classes are loaded
    try:
        for cls_name in CUSTOM_ROOFIT_CLASSES:
            cls = getattr(ROOT, cls_name)
    except AttributeError as exc:
        raise RuntimeError('could not load all custom RooFit classes: "{}"'.format(str(exc)))


_roo_cmd_args = []


def create_roofit_opts(opts_type='', create_linked_list=True, **kwargs):
    """Build list of options for RooFit functions

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
    if not opts_type in [None, '', 'fit', 'pdf_plot', 'data_plot', 'obs_frame']:
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
