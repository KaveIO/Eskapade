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

from . import decorators, style
from .roofit_manager import RooFitManager
from .links import *
