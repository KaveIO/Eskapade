/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Class  : RooTruncExponential                                              *
 * Created: 2015/06/03                                                       *
 * Description:                                                              *
 *      Exponential probability-density function, truncated at a variable    *
 *      value.  The function is implemented as a RooFit PDF.                 *
 *                                                                           *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                      *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

// esroofit includes.
#include <esroofit/RooTruncExponential.h>
#include <esroofit/RooComplementCoef.h>

// RooFit includes.
#include <RooRealVar.h>
#include <RooRandom.h>

// STL includes.
#include <algorithm>

//_____________________________________________________________________________
RooTruncExponential::RooTruncExponential(const char *name, const char *title,
                                         RooRealVar &var, RooAbsReal &maxVar, const RooArgList &exp,
                                         const RooArgList &fracs) :
        RooAbsPdf(name, title),
        _var("var", "variable", this, var),
        _maxVar("maxVar", "maximum of variable", this, maxVar),
        _exp("exp", "exponents", this),
        _fracs("fracs", "fractions", this),
        _lastFrac("lastFrac", "last of the fractions", this),
        _ignoreMax(kFALSE)
{
    assert(exp.getSize() == fracs.getSize() + 1);
    assert(!maxVar.dependsOn(var));

    RooFIter argIter(exp.fwdIterator());
    RooAbsArg *arg(0);
    while ((arg = argIter.next()) != 0)
    {
        assert(dynamic_cast<RooAbsReal *>(arg) != 0);
        assert(!var.dependsOn(*arg));
        _exp.add(*arg);
    }
    argIter = fracs.fwdIterator();
    while ((arg = argIter.next()) != 0)
    {
        assert(dynamic_cast<RooAbsReal *>(arg) != 0);
        assert(!var.dependsOn(*arg));
        _fracs.add(*arg);
    }
    RooComplementCoef *lastFrac(
            new RooComplementCoef("lastFrac", "last of the fractions", fracs));
    _lastFrac.setArg(*lastFrac);
    _fracs.add(*lastFrac);
}

//_____________________________________________________________________________
RooTruncExponential::RooTruncExponential(const RooTruncExponential &other,
                                         const char *name) :
        RooAbsPdf(other, name),
        _var("var", this, other._var),
        _maxVar("maxVar", this, other._maxVar),
        _exp("exp", this, other._exp),
        _fracs("fracs", this, other._fracs),
        _lastFrac("lastFrac", "last of the fractions", this),
        _ignoreMax(other._ignoreMax)
{}

//_____________________________________________________________________________
RooTruncExponential::~RooTruncExponential()
{
    RooComplementCoef *lastFrac
            = dynamic_cast<RooComplementCoef *>(_lastFrac.absArg());
    if (lastFrac != 0)
    { delete lastFrac; }
}

//_____________________________________________________________________________
Double_t RooTruncExponential::evaluate() const
{
    // the function is truncated at the value of "maxVar"
    if (!_ignoreMax && _var > _maxVar)
    { return 0.; }

    // return the sum of the exponential terms in the function
    Double_t sumTerms(0.);
    RooFIter expIter(_exp.fwdIterator());
    RooFIter fracIter(_fracs.fwdIterator());
    RooAbsReal *exp(0);
    while ((exp = (RooAbsReal *) expIter.next()) != 0)
    {
        // add "frac_i * |exp_i| * e^(exp_i * var)" to the sum
        RooAbsReal *frac((RooAbsReal *) fracIter.next());
        sumTerms += frac->getVal() * std::fabs(exp->getVal())
                    * std::exp(exp->getVal() * _var);
    }
    return sumTerms;
}

//_____________________________________________________________________________
Int_t RooTruncExponential::getAnalyticalIntegral(RooArgSet &allVars,
                                                 RooArgSet &analVars, const char * /*rangeName*/) const
{
    if (matchArgs(allVars, analVars, _var))
    { return 1; }
    return 0;
}

//_____________________________________________________________________________
Double_t RooTruncExponential::analyticalIntegral(Int_t code,
                                                 const char *rangeName) const
{
    // check integration code
    assert(code == 1);

    // get integration range
    Double_t varMin = _var.min(rangeName);
    Double_t varMax = _var.max(rangeName);;
    if (!_ignoreMax)
    { varMax = std::min(varMax, (Double_t) _maxVar); }

    // return the sum of the integrals of the exponential terms in the function
    Double_t sumTerms(0.);
    RooFIter expIter(_exp.fwdIterator());
    RooFIter fracIter(_fracs.fwdIterator());
    RooAbsReal *exp(0);
    while ((exp = (RooAbsReal *) expIter.next()) != 0)
    {
        // add contribution of each term to the sum:
        // frac_i * |exp_i| / exp_i * [e^(exp_i * var_max) - e^(exp_i * var_min)]
        RooAbsReal *frac((RooAbsReal *) fracIter.next());
        Double_t expInt(0.);
        if (exp->getVal() != 0.)
        {
            expInt = (exp->getVal() < 0. ? -1. : 1.) *
                     (std::exp(exp->getVal() * varMax) - std::exp(exp->getVal() * varMin));
        }
        sumTerms += frac->getVal() * expInt;
    }
    return sumTerms;
}

//_____________________________________________________________________________
Int_t RooTruncExponential::getGenerator(const RooArgSet &directVars,
                                        RooArgSet &generateVars, Bool_t /*staticInitOK*/) const
{
    if (matchArgs(directVars, generateVars, _var))
    { return 1; }
    return 0;
}

//_____________________________________________________________________________
void RooTruncExponential::generateEvent(Int_t code)
{
    // check generation code
    assert(code == 1);

    // get normalization range
    Double_t varMin = _var.min();
    Double_t varMax = _var.max();;
    if (!_ignoreMax)
    { varMax = std::min(varMax, (Double_t) _maxVar); }

    // get PDF fractions in normalization range
    std::vector<Double_t> normInts;
    Double_t sumTerms(0.);
    RooFIter expIter(_exp.fwdIterator());
    RooFIter fracIter(_fracs.fwdIterator());
    RooAbsReal *exp(0);
    while ((exp = (RooAbsReal *) expIter.next()) != 0)
    {
        // calculate contribution to normalization integral for each term:
        // frac_i * |exp_i| / exp_i * [e^(exp_i * var_max) - e^(exp_i * var_min)]
        assert(exp->getVal() != 0.);
        RooAbsReal *frac((RooAbsReal *) fracIter.next());
        normInts.push_back(frac->getVal() * (exp->getVal() < 0. ? -1. : 1.) *
                           (std::exp(exp->getVal() * varMax) - std::exp(exp->getVal() * varMin)));
        sumTerms += normInts.back();
    }

    // determine which PDF to use to generate event, based on fractions
    Int_t termPos(-1);
    Double_t termSum(0.);
    Double_t genIntVal(RooRandom::uniform() * sumTerms);
    for (std::vector<Double_t>::const_iterator termIt = normInts.begin();
         termIt != normInts.end(); ++termIt)
    {
        ++termPos;
        termSum += *termIt;
        if (genIntVal <= termSum)
        { break; }
    }

    // generate event with the selected PDF
    Double_t expVal(((RooAbsReal *) _exp.at(termPos))->getVal());
    assert(expVal != 0.);
    genIntVal = RooRandom::uniform();
    _var = std::log(std::exp(expVal * varMin) * (1. - genIntVal)
                    + std::exp(expVal * varMax) * genIntVal) / expVal;
}
