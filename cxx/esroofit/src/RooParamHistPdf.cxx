/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Class  : RooParamHistPdf                                                  *
 * Created: 2017/06/03                                                       *
 * Description:                                                              *
 *      Pdf class derived from RooHistPdf, which multiplies each bin
 *      with a RooRealVar scale parameter, e.g. which can be fit or
 *      constrained.
 *
 *      By default these parameters are normalized wrt the bin count,
 *      except when the number of entries in a bin is zero, in which case
 *      the parameter is the number of entries in the bin.
 *      Request these parameters with the function: paramList()
 *      The generation of the scale parameters can be turned off with the
 *      flag: noParams (default is false).
 *
 *      RooParamHistPdf works internally with another RooDataHist
 *      (modified for speedup), which gets updated whenever a parameter
 *      value is changed. Several RooHistPdf functions have been overloaded
 *      to pick up this modified roodatahist object instead.
 *      This internal roodatahist can be updated at any time with the
 *      function setModifiedData(), which requires noParams = false.
 *                                                                           *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                      *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

// esroofit includes.
#include <esroofit/RooParamHistPdf.h>


// ROOT includes.
#include <Riostream.h>
#include <TMath.h>

// RooFit includes.
#include <RooRealVar.h>
#include <RooWorkspace.h>
#include <RooHistError.h>
#include <RooConstVar.h>
#include <RooProduct.h>
#include <RooAddition.h>
#include <RooChangeTracker.h>


using namespace std;

ClassImp(RooParamHistPdf)


////////////////////////////////////////////////////////////////////////////////
/// Constructor from a RooDataHist. RooDataHist dimensions
/// can be either real or discrete. See RooDataHist::RooDataHist for details on the binning.
/// RooParamHistPdf neither owns or clone 'dhist' and the user must ensure the input histogram exists
/// for the entire life span of this PDF.

RooParamHistPdf::RooParamHistPdf(const char *name, const char *title, const RooArgSet &vars,
                                 const RooDataHist &dhist, Int_t intOrder, Bool_t noParams, Bool_t relParams) :
//RooAbsPdf(name,title),
        RooHistPdf(name, title, vars, dhist, intOrder),
        _p("p", "p", this),
        _c("c", "c", this),
        _b("b", "b", this),
        _t("t", "t", this),
        _dh(dhist),
        _dh_mod(0),
        _noParams(noParams),
        _relParams(relParams),
        _sumWnorm(dhist.sumEntries())
{
    _dh_mod = new FastHist(_dh);
    // nasty! b/c of ref to _dh, prevent double deletion at exit()
    _dh_mod->removeSelfFromDir();

    // Now populate _b with bin entries
    // Make poisson parameters representing each bin
    if (!_noParams)
    { // params on
        createBinParameters();
    }

    // keep track of changes in gamma parameters (if any)
    RooChangeTracker *tracker = new RooChangeTracker("tracker", "track gamma parameters", _p, true);
    if (!_noParams)
    {
        tracker->hasChanged(true);
    } // first evaluation always true for new parameters
    _t.add(*tracker);

    RooArgSet allVars(*tracker);
    addOwnedComponents(allVars);

    // set nominal expected datahist
    if (!_noParams)
    {
        updateModifiedData();
    }
}


////////////////////////////////////////////////////////////////////////////////
/// Copy constructor

RooParamHistPdf::RooParamHistPdf(const RooParamHistPdf &other, const char *name) :
//RooAbsPdf(other,name),
        RooHistPdf(other, name),
        _p("p", this, other._p),
        _c("c", this, other._c),
        _b("g", this, other._b),
        _t("t", this, other._t),
        _dh(other._dh),
        _dh_mod(0),
        _noParams(other._noParams),
        _relParams(other._relParams),
        _sumWnorm(other._sumWnorm)
{
    if (other._dh_mod != 0)
    {
        _dh_mod = new FastHist(*other._dh_mod);
        // nasty! b/c of ref to _dh, prevent double deletion at exit()
        _dh_mod->removeSelfFromDir();
    }

    if (!_noParams)
    {
        RooChangeTracker *tracker = ((RooChangeTracker *) _t.at(0));
        tracker->hasChanged(true); // first evaluation always true for new parameters
    }
}


////////////////////////////////////////////////////////////////////////////////
/// deconstructor

RooParamHistPdf::~RooParamHistPdf()
{
    if (_dh_mod != NULL)
    {
        delete _dh_mod;
        _dh_mod = NULL;
    }
}


////////////////////////////////////////////////////////////////////////////////
void
RooParamHistPdf::createBinParameters()
{
    // Now populate _b with bin entries
    // Make poisson parameters representing each bin

    RooArgSet allVars;

    for (Int_t i = 0; i < _dataHist->numEntries(); i++)
    {
        _dataHist->get(i);
        // floating parameters
        const char *vname = Form("%s_gamma_bin_%i", GetName(), i);
        RooRealVar *var = new RooRealVar(vname, vname, 1, 0, 6);
        var->setConstant(kTRUE);
        _p.add(*var);
        // c below is the inverse of p
        const char *cname = Form("%s_const_bin_%i", GetName(), i);
        RooRealVar *c = new RooRealVar(cname, cname, 0);
        c->setConstant(kTRUE);
        _c.add(*c);
        // now product: p*c
        // the product equals the value of the bin and contain one floatable parameter,
        // which is what we will use in this class in the end
        RooAbsArg *p = _p.at(i);
        const char *bname = Form("%s_bin_entries_%i", GetName(), i);
        RooProduct *bini_entries = new RooProduct(bname, bname, RooArgList(*c, *p));
        _b.add(*bini_entries);
        // collect all owned components
        allVars.add(*var);
        allVars.add(*c);
        allVars.add(*bini_entries);
    }

    addOwnedComponents(allVars);

    // set p and c values, errors and ranges
    setNominalData(_dh, false); // do not update dh_mod
}


////////////////////////////////////////////////////////////////////////////////
Double_t
RooParamHistPdf::getSumW() const
{
    if (_noParams)
    { return _sumWnorm; }

    Double_t sumW(0.);
    for (Int_t i = 0; i < _b.getSize(); i++)
    {
        Double_t w = ((RooAbsReal *) _b.at(i))->getVal();
        sumW += w;
    }

    return sumW;
}


////////////////////////////////////////////////////////////////////////////////
void
RooParamHistPdf::updateModifiedData() const
{
    Double_t sumW_current = this->getSumW();

    for (Int_t i = 0; i < _b.getSize(); i++)
    {
        Double_t w = ((RooAbsReal *) _b.at(i))->getVal();
        w *= (_sumWnorm / sumW_current);
        _dh_mod->setWeight(i, w);
        _dh_mod->setSumW2(i, w * w);
    }
}


////////////////////////////////////////////////////////////////////////////////
Double_t RooParamHistPdf::getEntries(Int_t ibin)
{
    RooChangeTracker *tracker = ((RooChangeTracker *) _t.at(0));
    if (tracker->hasChanged(kTRUE))
    {
        this->updateModifiedData();
    }

    if (ibin >= 0 && ibin < _dh_mod->numEntries())
    {
        _dh_mod->get(ibin);
        return _dh_mod->weight();
    }
    return -999;
}


////////////////////////////////////////////////////////////////////////////////
Double_t RooParamHistPdf::getEntriesError(Int_t ibin) const
{
    Double_t sumW_current = getSumW();
    if (ibin >= 0 && ibin < _b.getSize())
    {
        Double_t y = ((RooRealVar &) _b[ibin]).getVal();
        Double_t ym1, yp1;
        RooHistError::instance().getPoissonInterval(y, ym1, yp1, 1);
        Double_t yerr = (yp1 - ym1) / 2.;
        return yerr * (_sumWnorm / sumW_current);
    }
    return -999;
}


////////////////////////////////////////////////////////////////////////////////
Double_t RooParamHistPdf::getNominal(Int_t ibin) const
{
    if (ibin >= 0 && ibin < _dh.numEntries())
    {
        _dh.get(ibin);
        return _dh.weight();
    }
    return -999;
}


////////////////////////////////////////////////////////////////////////////////
Double_t RooParamHistPdf::getNominalError(Int_t ibin) const
{
    if (ibin >= 0 && ibin < _dh.numEntries())
    {
        _dh.get(ibin);
        return _dh.weightError();
    }
    return -999;
    //_dh.get(ibin) ;
    //return _dh.weightError() ;
}


////////////////////////////////////////////////////////////////////////////////
void RooParamHistPdf::setAllGammaConstant(Bool_t constant)
{
    for (Int_t i = 0; i < _p.getSize(); i++)
        ((RooRealVar &) _p[i]).setConstant(constant);
}


////////////////////////////////////////////////////////////////////////////////
void RooParamHistPdf::setGammaConstant(Int_t i, Bool_t constant)
{
    if (i >= 0 && i < _p.getSize())
    {
        ((RooRealVar &) _p[i]).setConstant(constant);
    }
}


////////////////////////////////////////////////////////////////////////////////
/// Return the current value: The value of the bin enclosing the current coordinates
/// of the observables, normalized by the histograms contents. Interpolation
/// is applied if the RooParamHistPdf is configured to do that

Double_t RooParamHistPdf::evaluate() const
{
    // hack: overhead, but this ensures synching and copying of caches through base class
    (void) this->RooHistPdf::evaluate();

    // check if transfering of hist vs pdf variables was okay
    if (_pdfObsList.getSize() > 0)
    {
        _histObsIter->Reset();
        _pdfObsIter->Reset();
        RooAbsArg *harg, *parg;
        while ((harg = (RooAbsArg *) _histObsIter->Next()))
        {
            parg = (RooAbsArg *) _pdfObsIter->Next();
            if (harg != parg)
            {
                if (!harg->inRange(0))
                {
                    return 0;
                }
            }
        }
    }

    // update expected data in case of changes to gamma parameters
    RooChangeTracker *tracker = ((RooChangeTracker *) _t.at(0));
    if (tracker->hasChanged(kTRUE))
    {
        this->updateModifiedData();
    }

    Double_t ret = _dh_mod->weight(_histObsList, _intOrder, _unitNorm ? kFALSE : kTRUE, _cdfBoundaries);
    if (ret < 0)
    { ret = 0; }
    return ret;
}


const RooDataHist &
RooParamHistPdf::getModifiedData() const
{
    RooChangeTracker *tracker = ((RooChangeTracker *) _t.at(0));
    if (tracker->hasChanged(kTRUE))
    {
        this->updateModifiedData();
    }

    return *_dh_mod;
}


////////////////////////////////////////////////////////////////////////////////
/// Return integral identified by 'code'. The actual integration
/// is deferred to RooDataHist::sum() which implements partial
/// or complete summation over the histograms contents

Double_t RooParamHistPdf::analyticalIntegral(Int_t code, const char *rangeName) const
{
    RooChangeTracker *tracker = ((RooChangeTracker *) _t.at(0));
    if (tracker->hasChanged(kTRUE))
    {
        this->updateModifiedData();
    }

    // hack: overhead, but this ensures synching and copying of caches through base class
    (void) this->RooHistPdf::analyticalIntegral(code, rangeName);

    // Simplest scenario, full-range integration over all dependents
    if (((2 << _histObsList.getSize()) - 1) == code)
    {
        return _dh_mod->sum(kFALSE);
    }

    // Partial integration scenario, retrieve set of variables, calculate partial
    // sum, figure out integration ranges (if needed)
    RooArgSet intSet;
    std::map<const RooAbsArg *, std::pair<Double_t, Double_t> > ranges;
    RooFIter it = _pdfObsList.fwdIterator();
    RooFIter jt = _histObsList.fwdIterator();
    Int_t n(0);
    for (RooAbsArg *pa = 0, *ha = 0; (pa = it.next()) && (ha = jt.next()); ++n)
    {
        if (code & (2 << n))
        {
            intSet.add(*ha);
        }
        if (!(code & 1))
        {
            RooAbsRealLValue *rlv = dynamic_cast<RooAbsRealLValue *>(pa);
            if (rlv)
            {
                const RooAbsBinning *binning = rlv->getBinningPtr(rangeName);
                if (rangeName && rlv->hasRange(rangeName))
                {
                    ranges[ha] = std::make_pair(
                            rlv->getMin(rangeName), rlv->getMax(rangeName));
                }
                else if (binning)
                {
                    if (!binning->isParameterized())
                    {
                        ranges[ha] = std::make_pair(
                                binning->lowBound(), binning->highBound());
                    }
                    else
                    {
                        ranges[ha] = std::make_pair(
                                binning->lowBoundFunc()->getVal(), binning->highBoundFunc()->getVal());
                    }
                }
            }
        }
    }

    Double_t ret = (code & 1) ?
                   _dh_mod->sum(intSet, _histObsList, kTRUE, kTRUE) :
                   _dh_mod->sum(intSet, _histObsList, kFALSE, kTRUE, ranges);

    return ret;
}


////////////////////////////////////////////////////////////////////////////////
Bool_t RooParamHistPdf::hasTrackerChanged() const
{
    RooChangeTracker *tracker = ((RooChangeTracker *) _t.at(0));
    return tracker->hasChanged(kTRUE);
}


////////////////////////////////////////////////////////////////////////////////
Double_t RooParamHistPdf::maxVal(Int_t code) const
{
    R__ASSERT(code == 1);

    // update expected data in case of changes to gamma parameters
    RooChangeTracker *tracker = ((RooChangeTracker *) _t.at(0));
    if (tracker->hasChanged(kTRUE))
    {
        this->updateModifiedData();
    }

    Double_t max(-1);
    for (Int_t i = 0; i < _dh_mod->numEntries(); i++)
    {
        _dh_mod->get(i);
        Double_t wgt = _dh_mod->weight();
        if (wgt > max)
        { max = wgt; }
    }

    return max * 1.05;
}


////////////////////////////////////////////////////////////////////////////////
void RooParamHistPdf::setNominalData(const RooDataHist &nomHist, Bool_t updateModifiedData)
{
  if (_noParams) return; // nothing to set

  R__ASSERT( _dh.numEntries()==nomHist.numEntries() );
  R__ASSERT( _c.getSize()==nomHist.numEntries() );
  R__ASSERT( _p.getSize()==nomHist.numEntries() );

  // check that all required observables exist in dataset
  const RooArgSet* vars = nomHist.get();
  const RooArgSet* obsSet = _dh.get();
  R__ASSERT( vars->getSize()==obsSet->getSize() );

  TIterator* obsItr = obsSet->createIterator();
  RooAbsArg* obs ;
  for (Int_t j=0; (obs = (RooAbsArg*)obsItr->Next()); ++j) {
    RooAbsArg* var = vars->find( obs->GetName() );
    R__ASSERT( var!=0 );
  }
  delete obsItr;

  // MB: below I am assuming the dhists have identical binnings.

  for (Int_t i=0; i<_dh.numEntries(); i++) {
    // retrieve data
    const RooArgSet* x = _dh.get(i);
    nomHist.get(*x);
    Double_t w = nomHist.weight();
    //Double_t we = nomHist.weightError();
    // 1. update constants
    RooRealVar& c = (RooRealVar&)_c[i];
    c.setError(0);
    c.setConstant();
    if (_relParams && (w>0)) {
      c.setRange(w,w);
      c.setVal(w);
    } else {
      // if nobs==0: c becomes relative, and p absolute
      c.setRange(1,1);
      c.setVal(1);
    }
    // 2. update floating parameters
    RooRealVar& p = (RooRealVar&)_p[i];
    Double_t y(w), ym1, yp1, yerr, yerrm, yerrp;
    RooHistError::instance().getPoissonInterval(y,ym1,yp1,1);
    yerr = (yp1-ym1)/2.;
    yerrm = ym1-y;
    yerrp = yp1-y;
    if (_relParams && (w>0)) {
      p.setVal(1) ;
      yerr /= y;
      yerrm /= y;
      yerrp /= y;
    } else { // absolute parameters or w==0
      p.setVal(w);
    }
    Double_t pMin = p.getVal() - 10.*TMath::Abs(yerrm);
    if (pMin<=0) { pMin = 0.; }
    Double_t pMax = p.getVal() + 10.*yerrp;
    if (_relParams && pMax>6) { pMax = 6.; }
    p.setRange( pMin, pMax );
    p.setAsymError( yerrm, yerrp );
    p.setError( yerr ) ;
  }

  _sumWnorm = this->getSumW();

  // update internal histogram
  if (updateModifiedData)
    this->updateModifiedData();
  else {
    // try update of matrix later
    RooChangeTracker* tracker = ((RooChangeTracker*)_t.at(0));
    if (tracker!=0)
      tracker->hasChanged(true);
  }
}
