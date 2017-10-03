# flake8: noqa
try:
    import ROOT
except ImportError:
    from eskapade import MissingRootError

    raise MissingRootError()

try:
    from ROOT import RooFit
except ImportError:
    try:
        # noinspection PyUnresolvedReferences
        import ROOT.RooFit
    except ImportError:
        from eskapade import MissingRooFitError

        raise MissingRooFitError()

try:
    from ROOT import RooStats
except ImportError:
    try:
        # noinspection PyUnresolvedReferences
        import ROOT.RooStats
    except ImportError:
        from eskapade import MissingRooStatsError

        raise MissingRooStatsError()

from eskapade.root_analysis import decorators, style
from eskapade.root_analysis.links import *
from eskapade.root_analysis.roofit_manager import RooFitManager
