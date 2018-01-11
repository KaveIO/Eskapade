/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Class  : RooABCDHistPdf                                                   *
 * Created: 2017/06/03                                                       *
 *
 * Description:                                                              *
 *      Pdf class derived from RooParamHistPdf, which constructs a
 *      RooHistPdf based on the input datahist object that is factorized in
 *      all given input observables. I.e. it explicitly assumes that all
 *      input observables are uncorrelated, and makes a pdf that best fits that
 *      hypothesis. This factorization is applied to both RooRealVar and
 *      RooCategory based observables.
 *
 *      This pdf can be used to test the factorization assumption of
 *      observables in arbitrary datasets, and to find the most dominant
 *      outliers in this assumptions.
 *      Examples for this are given in the module ABCDUtils(.cxx/.h)
 *
 *      The factorization can be done in two ways, controlled with the
 *      parameter doABCD, giving:
 *      1. A prediction for bin i that includes in its calculation the
 *         nominal bin entries of bin i (doABCD=false).
 *      2. A prediction for bin i that excludes in its calculation the
 *         nominal bin entries of bin i (doABCD=true).
 *
 *      To illustrate, assume a 2x2 input histogram corresponding to two
 *      observables with two bins for each observable. The bins are labelled:
 *      ( (A, B),
 *        (C, D) )
 *
 *      The two factorization methods work as follows:
 *      1. Prediction for bin A = ((A+B)*(A+C)) / (A+B+C+D)
 *         See for more details:
 *         https://en.wikipedia.org/wiki/Contingency_table
 *      2. Prediction for bin A = (B/D) * (C/D) * D = (B*C) / D
 *
 *      The generation of the scale RooRealVar parameters, parametrizing the
 *      Poisson uncertainties on the input bins, can be turned off with the
 *      flag: noParams (default is false).
 *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                      *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

// esroofit includes.
#include <esroofit/RooABCDHistPdf.h>
#include <esroofit/TMsgLogger.h>

// ROOT includes.
#include <TMath.h>
#include <Riostream.h>

// RooFit includes.
#include <RooRealVar.h>
#include <RooCategory.h>
#include <RooWorkspace.h>
#include <RooHistError.h>
#include <RooConstVar.h>
#include <RooProduct.h>
#include <RooAddition.h>
#include <RooChangeTracker.h>
#include <RooDataSet.h>


using namespace std;

ClassImp(RooABCDHistPdf)

////////////////////////////////////////////////////////////////////////////////
/// Constructor from a RooDataHist. RooDataHist dimensions
/// can be either real or discrete. See RooDataHist::RooDataHist for details on the binning.
/// RooABCDHistPdf neither owns or clone 'dhist' and the user must ensure the input histogram exists
/// for the entire life span of this PDF.

RooABCDHistPdf::RooABCDHistPdf(const char *name, const char *title, const RooArgSet &vars,
                               const RooDataHist &dhist, Bool_t doABCD, Bool_t noParams) :
        RooParamHistPdf(name, title, vars, dhist, 0, noParams),
        _s("s", "s", this),
        _f("f", "f", this),
        _abcd(doABCD),
        _ndim(0),
        _allBCDvec(new std::vector<double>())
{
    _ndim = int(vars.getSize());

    // perform basic checks on input data
    Bool_t success = Eskapade::ABCD::checkInputData(_dh, _abcd);
    R__ASSERT(success);

    // construct expected data functions
    if (_noParams)
    {
        this->calculateData(_dh_mod, _abcd);
        // update sum of weights for normalization
        _sumWnorm = _dh_mod->sumEntries();
    }
    else
    {
        constructExpectationFunctions();
        // update sum of weights for normalization
        _sumWnorm = this->getSumW();
        // set nominal expected datahist
        updateModifiedData();
    }
}


////////////////////////////////////////////////////////////////////////////////
/// Copy constructor

RooABCDHistPdf::RooABCDHistPdf(const RooABCDHistPdf &other, const char *name) :
        RooParamHistPdf(other, name),
        _s("s", this, other._s),
        _f("f", this, other._f),
        _abcd(other._abcd),
        _ndim(other._ndim),
        _allBCDvec(new std::vector<double>(*other._allBCDvec))
{
}


////////////////////////////////////////////////////////////////////////////////
/// deconstructor

RooABCDHistPdf::~RooABCDHistPdf()
{
    if (_allBCDvec != 0)
    {
        delete _allBCDvec;
        _allBCDvec = 0;
    }
}


////////////////////////////////////////////////////////////////////////////////
Double_t RooABCDHistPdf::getEntriesError(Int_t ibin) const
{
    if (!_abcd)
    { return -999; }
    if (!(ibin >= 0 && ibin < _dh.numEntries()))
    { return -999; }

    Int_t offset = (_abcd ? 0 : 1);
    Int_t ns = (_abcd ? 1 : 0) + _ndim + (_noParams ? 0 : 2);
    Int_t idx_d = (_abcd ? ibin * ns : 0);
    Int_t offset_bc = offset + ibin * ns + (_abcd ? 1 : 0);

    // container for BCD values
    std::vector<Double_t> bcdVec(_ndim + 1);
    // set BC values
    for (Int_t j = 0; j < _ndim; j++)
        bcdVec[j] = _noParams ? _allBCDvec->at(offset_bc + j) : ((RooRealVar &) _s[offset_bc + j]).getVal();
    // set D value at last element
    bcdVec[_ndim] = _noParams ? _allBCDvec->at(idx_d) : ((RooRealVar &) _s[idx_d]).getVal();

    // copy of BCD values, for error-propagation modifications
    std::vector<Double_t> bcdCopy(bcdVec);
    Double_t sigmaA(0.);
    // apply varations of BCD to A
    for (Int_t j = 0; j < _ndim + 1; j++)
    {
        Double_t y = bcdVec[j];
        Double_t ym1, yp1;
        RooHistError::instance().getPoissonInterval(y, ym1, yp1, 1);
        bcdCopy[j] = yp1;
        Double_t Ap1 = this->abcdFunction(bcdCopy);
        bcdCopy[j] = ym1;
        Double_t Am1 = this->abcdFunction(bcdCopy);
        // store 1 sigma variation
        Double_t dA = (Ap1 - Am1) / 2.;
        sigmaA += dA * dA;
        // reset for next iteration
        bcdCopy[j] = y;
    }
    sigmaA = TMath::Sqrt(sigmaA);

    Double_t sumW_current = getSumW();
    return sigmaA * (_sumWnorm / sumW_current);
}


////////////////////////////////////////////////////////////////////////////////
Double_t
RooABCDHistPdf::getSumW() const
{
    if (_noParams)
    { return _sumWnorm; }

    Double_t sumW(0.);
    for (Int_t i = 0; i < _f.getSize(); i++)
    {
        Double_t w = ((RooAbsReal *) _f.at(i))->getVal();
        sumW += w;
    }

    return sumW;
}


////////////////////////////////////////////////////////////////////////////////
void
RooABCDHistPdf::updateModifiedData() const
{
    Double_t sumW_current = this->getSumW();

    for (Int_t i = 0; i < _f.getSize(); i++)
    {
        Double_t w = ((RooAbsReal *) _f.at(i))->getVal();
        // renormalize weight to ensure sumW does not diverge
        w *= (_sumWnorm / sumW_current);
        _dh_mod->setWeight(i, w);
        _dh_mod->setSumW2(i, w * w);
    }
}


////////////////////////////////////////////////////////////////////////////////
void
RooABCDHistPdf::calculateData(FastHist *dh_modify_me, Bool_t abcd) const
{
    const RooArgSet *vars = _dh.get();
    TIterator *varItr = vars->createIterator();

    // now check that datahist is properly filled for abcd method.
    std::vector<std::string> cuts(_ndim), inv_cuts(_ndim), bc_cuts(_ndim);
    std::string d_cut;

    // create sums in B,C,D regions, needed for A = B*C / D expectation formula.
    _allBCDvec->clear();

    // full D region sum
    Double_t sumD = _dh.sumEntries();
    if (!abcd)
    {
        _allBCDvec->push_back(sumD);
    }

    for (Int_t i = 0; i < _dh.numEntries(); i++)
    {
        // update vars
        _dh.get(i);
        varItr->Reset();

        // 0. create basic selection cuts for factorization
        RooAbsArg *p;
        for (Int_t j = 0; (p = (RooAbsArg *) varItr->Next()); ++j)
        {
            if (dynamic_cast<RooRealVar *>(p))
            {
                RooRealVar *var = static_cast<RooRealVar *>(p);
                const RooAbsBinning &binning = var->getBinning();
                Int_t b_idx = binning.rawBinNumber(var->getVal());
                Double_t b_lo = binning.binLow(b_idx);
                Double_t b_hi = binning.binHigh(b_idx);
                cuts[j] = Form("(%s>=%f && %s<%f)", p->GetName(), b_lo, p->GetName(), b_hi);
                inv_cuts[j] = Form("(%s<%f || %s>=%f)", p->GetName(), b_lo, p->GetName(), b_hi);
            }
            if (dynamic_cast<RooCategory *>(p))
            {
                RooCategory *cat = static_cast<RooCategory *>(p);
                cuts[j] = Form("(%s==%s::%s)", p->GetName(), p->GetName(), cat->getLabel());
                inv_cuts[j] = Form("(%s!=%s::%s)", p->GetName(), p->GetName(), cat->getLabel());
            }
        }

        // 1. construct D region sum
        Double_t sumD_i(0);
        if (abcd)
        {
            d_cut = "";
            for (Int_t j = 0; j < _ndim; ++j)
            {
                if (j != 0)
                { d_cut += " && "; }
                d_cut += inv_cuts[j];
            }
            sumD_i = _dh.sumEntries(d_cut.c_str());
            _allBCDvec->push_back(sumD_i);
        }
        else
        {
            sumD_i = sumD;
        }

        // 2. construct B,C region sums
        Double_t bcdd_frac_product_i(1.);
        for (Int_t j = 0; j < _ndim; ++j)
        {
            bc_cuts[j] = "";
            if (abcd)
            {
                for (Int_t k = 0; k < _ndim; ++k)
                {
                    if (k != 0)
                    { bc_cuts[j] += " && "; }
                    if (k == j)
                    {
                        bc_cuts[j] += cuts[k];
                    }
                    else
                    {
                        bc_cuts[j] += inv_cuts[k];
                    }
                }
            }
            else
            {
                bc_cuts[j] += cuts[j];
            }
            Double_t sumBC_ij = _dh.sumEntries(bc_cuts[j].c_str());
            _allBCDvec->push_back(sumBC_ij);
            bcdd_frac_product_i *= (sumBC_ij / sumD_i);
        }

        // 3. expectation for A_i
        Double_t Ai = bcdd_frac_product_i * sumD_i;
        dh_modify_me->setWeight(i, Ai);
        dh_modify_me->setSumW2(i, Ai * Ai);
    }

    delete varItr;
}


////////////////////////////////////////////////////////////////////////////////
void
RooABCDHistPdf::constructExpectationFunctions()
{
    const RooArgSet *vars = _dh.get();
    TIterator *varItr = vars->createIterator();

    // now check that datahist is properly filled for abcd method.
    std::vector<std::string> cuts(_ndim), inv_cuts(_ndim), bc_cuts(_ndim);
    std::string d_cut;

    RooArgSet allVars;

    RooAbsReal *sumAllFunc(0);
    if (!_abcd)
    {
        sumAllFunc = createSumEntriesFunc();
        _s.add(*sumAllFunc);
        allVars.add(*sumAllFunc);
    }

    // create sums in B,C,D regions, needed for A = B*C / D formula.
    for (Int_t i = 0; i < _dh.numEntries(); i++)
    {
        // update vars
        _dh.get(i);
        varItr->Reset();

        if (i % 100 == 0)
            coutI(InputArguments) << "RooABCDHistPdf::constructExpectationFunctions(" << GetName()
                                  << ") INFO: processing bin " << i << endl;

        // 0. create basic selection cuts for factorization
        RooAbsArg *p;
        for (Int_t j = 0; (p = (RooAbsArg *) varItr->Next()); ++j)
        {
            if (dynamic_cast<RooRealVar *>(p))
            {
                RooRealVar *var = static_cast<RooRealVar *>(p);
                const RooAbsBinning &binning = var->getBinning();
                Int_t b_idx = binning.rawBinNumber(var->getVal());
                Double_t b_lo = binning.binLow(b_idx);
                Double_t b_hi = binning.binHigh(b_idx);
                cuts[j] = Form("(%s>=%f && %s<%f)", p->GetName(), b_lo, p->GetName(), b_hi);
                inv_cuts[j] = Form("(%s<%f || %s>=%f)", p->GetName(), b_lo, p->GetName(), b_hi);
            }
            if (dynamic_cast<RooCategory *>(p))
            {
                RooCategory *cat = static_cast<RooCategory *>(p);
                cuts[j] = Form("(%s==%s::%s)", p->GetName(), p->GetName(), cat->getLabel());
                inv_cuts[j] = Form("(%s!=%s::%s)", p->GetName(), p->GetName(), cat->getLabel());
            }
        }

        // 1. construct D region sum
        RooAbsReal *sumD_i(0);
        if (_abcd)
        {
            d_cut = "";
            for (Int_t j = 0; j < _ndim; ++j)
            {
                if (j != 0)
                { d_cut += " && "; }
                d_cut += inv_cuts[j];
            }
            std::string d_name = Form("sum_D_%d", i);
            sumD_i = createSumEntriesFunc(d_name.c_str(), d_cut.c_str());
            _s.add(*sumD_i);
            allVars.add(*sumD_i);
        }
        else
        {
            sumD_i = sumAllFunc;
        }
        RooArgList dList;
        for (Int_t j = 0; j < _ndim - 1; ++j)
        { dList.add(*sumD_i); }

        // 2. construct B,C region sums
        RooArgList bcList;
        for (Int_t j = 0; j < _ndim; ++j)
        {
            bc_cuts[j] = "";
            if (_abcd)
            {
                for (Int_t k = 0; k < _ndim; ++k)
                {
                    if (k != 0)
                    { bc_cuts[j] += " && "; }
                    if (k == j)
                    {
                        bc_cuts[j] += cuts[k];
                    }
                    else
                    {
                        bc_cuts[j] += inv_cuts[k];
                    }
                }
            }
            else
            {
                bc_cuts[j] += cuts[j];
            }
            std::string bc_name = Form("sum_BC_%d_%d", i, j);
            RooAbsReal *sumBC_ij = createSumEntriesFunc(bc_name.c_str(), bc_cuts[j].c_str());
            bcList.add(*sumBC_ij);
            _s.add(*sumBC_ij);
            allVars.add(*sumBC_ij);
        }

        // 3. calculate B*C, D, and A
        const char *Aname = Form("A_%d", i);
        const char *BCname = Form("BC_%d", i);
        const char *Dname = Form("D_%d", i);
        RooProduct *BCi = new RooProduct(BCname, BCname, bcList);
        RooProduct *Di = new RooProduct(Dname, Dname, dList);
        RooFormulaVar *Ai = new RooFormulaVar(Aname, Aname, "@0/@1", RooArgList(*BCi, *Di));
        _s.add(RooArgSet(*BCi, *Di));
        _f.add(*Ai);
        allVars.add(RooArgSet(*BCi, *Di, *Ai));
    }

    delete varItr;

    // keep all owned components
    addOwnedComponents(allVars);
}


////////////////////////////////////////////////////////////////////////////////
RooAbsReal *
RooABCDHistPdf::createSumEntriesFunc(const char *sumName, const char *cutSpec)
{
    const RooArgSet *vars = _dh.get();

    // Setup RooFormulaVar for cutSpec if it is present
    RooFormula *select = new RooFormula("select", cutSpec, *vars);

    // Otherwise sum the weights in the event
    RooArgList binList;

    for (Int_t i = 0; i < _dh.numEntries(); i++)
    {
        // update vars
        _dh.get(i);
        if (select && select->eval() == 0.)
        { continue; }
        binList.add(*_b.at(i));
    }
    delete select;

    RooAddition *sumFunc = new RooAddition(sumName, sumName, binList);

    // requester owns sumFunc
    return sumFunc;
}


////////////////////////////////////////////////////////////////////////////////
Double_t
RooABCDHistPdf::abcdFunction(const std::vector<Double_t> &bcdVec) const
{
    R__ASSERT(Int_t(bcdVec.size()) == _ndim + 1);

    Double_t A(1.);
    Double_t D = bcdVec[_ndim];

    for (Int_t j = 0; j < _ndim; ++j)
    {
        A *= (bcdVec[j] / D);
    }
    A *= D;
    return A;
}


////////////////////////////////////////////////////////////////////////////////
void
RooABCDHistPdf::setNominalData(const RooDataHist &nomHist, Bool_t updateModifiedData)
{
    Bool_t success = Eskapade::ABCD::checkInputData(nomHist, _abcd);
    R__ASSERT(success);

    RooParamHistPdf::setNominalData(nomHist, updateModifiedData);
}


////////////////////////////////////////////////////////////////////////////////
Bool_t Eskapade::ABCD::checkInputData(const RooDataHist &data, Bool_t abcd)
{
    Bool_t success(kTRUE);

    TMsgLogger logger("ABCDChecker");

    // check that datahist is properly filled.
    if (data.numEntries() == 0)
    {
        logger << kWARNING << "checkInputData() of " << data.GetName() << " : number of entries is zero." << GEndl;
        success = kFALSE;
    }
    if (data.sumEntries() == 0)
    {
        logger << kWARNING << "checkInputData() of " << data.GetName() << " : sum entries is zero." << GEndl;
        success = kFALSE;
    }

    const RooArgSet *vars = data.get();
    Int_t ndim = int(vars->getSize());
    R__ASSERT(ndim >= 2);

    Double_t min_num_entries = TMath::Power(2., Double_t(ndim));
    R__ASSERT(data.numEntries() >= min_num_entries);

    // check types of all observables in datahist
    TIterator *varItr = vars->createIterator();
    RooAbsArg *var;
    for (Int_t j = 0; (var = (RooAbsArg *) varItr->Next()); ++j)
    {
        if (dynamic_cast<RooRealVar *>(var))
        { continue; }
        if (dynamic_cast<RooCategory *>(var))
        { continue; }
        logger << kWARNING << "checkInputData() of " << data.GetName() << " : variable " << var->GetName()
               << " is neither a variable nor category." << GEndl;
        success = kFALSE;
    }

    if (!abcd)
    {
        delete varItr;
        return success;
    }

    // now check that datahist is properly filled for abcd method.
    std::vector<std::string> cuts(ndim); //cuts(ndim), inv_cuts(ndim);

    // For each entry, check that the D region (A = B*C/D) is not zero.
    for (Int_t i = 0; i < data.numEntries(); i++)
    {
        // update vars
        data.get(i);
        varItr->Reset();

        RooAbsArg *p;
        for (Int_t j = 0; (p = (RooAbsArg *) varItr->Next()); ++j)
        {
            if (dynamic_cast<RooRealVar *>(p))
            {
                RooRealVar *var = static_cast<RooRealVar *>(p);
                const RooAbsBinning &binning = var->getBinning();
                Int_t b_idx = binning.rawBinNumber(var->getVal());
                Double_t b_lo = binning.binLow(b_idx);
                Double_t b_hi = binning.binHigh(b_idx);
                //cuts[j] = Form("(%s>=%f && %s<%f)", p->GetName(), b_lo, p->GetName(), b_hi);
                cuts[j] = Form("(%s<%f || %s>=%f)", p->GetName(), b_lo, p->GetName(), b_hi);
            }
            if (dynamic_cast<RooCategory *>(p))
            {
                RooCategory *cat = static_cast<RooCategory *>(p);
                //cuts[j] = Form("(%s==%s::%s)", p->GetName(), p->GetName(), cat->getLabel());
                cuts[j] = Form("(%s!=%s::%s)", p->GetName(), p->GetName(), cat->getLabel());
            }
        }

        // sumi will be a denominator in a ratio. Check it's not zero for every bin.
        std::string cuti;
        for (Int_t j = 0; j < ndim; ++j)
        {
            if (j != 0)
            { cuti += " && "; }
            cuti += cuts[j];
        }
        Double_t sumi = data.sumEntries(cuti.c_str());
        if (sumi == 0)
        {
            logger << kWARNING << "checkInputData() of " << data.GetName() << " : sumEntries for bin " << i
                   << " is zero. Cannot apply ABCD formula." << GEndl;
            success = kFALSE;
        }
    }

    delete varItr;

    return success;
}
