# *****************************************************************************
# * Project: Eskapade - A python-based package for data analysis              *
# * Macro  : esk409_unredeemed_vouchers                                       *
# * Created: 2017/04/19                                                       *
# *                                                                           *
# * Description:                                                              *
# *     This macro is an example of an application of the truncated
# *     exponential PDF that is provided by Eskapade.  The redeem of
# *     gift vouchers by customers of a store is modelled.
# *
# *     Vouchers are given out to customers of the store and can be
# *     exchanged for goods sold in the store.  All vouchers represent
# *     the same amount of money and can only be used once.  They are
# *     given to customers in batches at different dates.
# *
# *     Not all released vouchers are actually spent.  To estimate how
# *     many currently released vouchers will be spent, the voucher age
# *     at which the redeem takes place is modelled by a
# *     double-exponential decay model.  The exponential PDF is
# *     truncated at the voucher age, beyond which there can have been
# *     no redeems yet. Once the parameters of the model have been fit
# *     to (generated) redeem-event data, the total number of redeems at
# *     infinite voucher ages is estimated by scaling to the surface of
# *     an untruncated PDF with identical parameter values.
# *
# * Authors:                                                                  *
# *     KPMG Big Data team, Amstelveen, The Netherlands                       *
# *                                                                           *
# * Redistribution and use in source and binary forms, with or without        *
# * modification, are permitted according to the terms listed in the file     *
# * LICENSE.                                                                  *
# *****************************************************************************

import logging
import numpy as np

from eskapade import ConfigObject, ProcessManager
from eskapade.root_analysis import RooFitManager, TruncExpGen, TruncExpFit, roofit_utils
from eskapade.root_analysis.roofit_models import TruncExponential
import ROOT

MODEL_NAME = 'voucher_redeem'
REDEEM_DATA_KEY = 'voucher_redeems'
AGE_DATA_KEY = 'voucher_ages'

MAX_AGE = 1500  # days
FAST_REDEEM_RATE = -0.01  # per day
SLOW_REDEEM_RATE = -0.001  # per day
FAST_FRAC = 0.4
REDEEM_FRAC = 0.6

log = logging.getLogger('macro.esk409_unredeemed_vouchers')
log.debug('Now parsing configuration file esk409_unredeemed_vouchers')


###############################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk409_unredeemed_vouchers'
settings['version'] = 0


###############################################################################
# --- create voucher redeem model

# create model if it is not read from persisted services of first chain
if not settings.get('beginWithChain'):
    rfm = proc_mgr.service(RooFitManager)
    model = rfm.model(MODEL_NAME, model_cls=TruncExponential, var_range=(0., MAX_AGE), var=('redeem_age', 0.),
                      max_var=('age', MAX_AGE), exp=[('rate_fast', FAST_REDEEM_RATE), ('rate_slow', SLOW_REDEEM_RATE)],
                      fracs=[('frac_fast', FAST_FRAC)])
    model.build_model()
    model.var.SetTitle('Redeem age')
    model.max_var.SetTitle('Age')
    model.var.setUnit('days')
    model.max_var.setUnit('days')


###############################################################################
# --- create chain for generating voucher redeem data

ch = proc_mgr.add_chain('Generation')
gen_link = TruncExpGen(name='Generate', store_key=REDEEM_DATA_KEY, max_var_data_key=AGE_DATA_KEY,
                       model_name=MODEL_NAME, event_frac=REDEEM_FRAC)
ch.add_link(gen_link)

np.random.seed(settings['seeds']['NumPy'])
ROOT.RooRandom.randomGenerator().SetSeed(settings['seeds']['RooFit'])


###############################################################################
# --- create chain for fitting voucher redeem model to generated data

ch = proc_mgr.add_chain('Fitting')
fit_link = TruncExpFit(name='Fit', read_key=gen_link.store_key, max_var_data_key=gen_link.max_var_data_key,
                       model_name=gen_link.model_name)
ch.add_link(fit_link)


###############################################################################

log.debug('Done parsing configuration file esk409_unredeemed_vouchers')
