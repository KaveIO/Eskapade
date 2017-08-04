try:
    import ROOT
except:
    from eskapade import MissingRootError
    raise MissingRootError()

try:
    from ROOT import RooFit
except:
    from eskapade import MissingRooFitError
    raise MissingRooFitError()

try:
    from ROOT import RooStats
except:
    from eskapade import MissingRooStatsError
    raise MissingRooStatsError()

from . import decorators, style
from .roofit_manager import RooFitManager
from .links import *
