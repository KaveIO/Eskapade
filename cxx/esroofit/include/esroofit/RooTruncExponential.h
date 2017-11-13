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

#ifndef ROO_TRUNC_EXPONENTIAL
#define ROO_TRUNC_EXPONENTIAL

// RooFit includes.
#include <RooAbsPdf.h>
#include <RooArgList.h>
#include <RooRealProxy.h>
#include <RooListProxy.h>

// RooFit forward declarations.
class RooAbsReal;
class RooRealVar;


class RooTruncExponential : public RooAbsPdf
{

public:
    RooTruncExponential()
    {};

    // constructor
    RooTruncExponential(const char *name, const char *title, RooRealVar &var,
                        RooAbsReal &maxVar, const RooArgList &exp, const RooArgList &fracs);

    // copy constructor
    RooTruncExponential(const RooTruncExponential &other, const char *name = 0);

    virtual TObject *clone(const char *name) const
    {
        return new RooTruncExponential(*this, name);
    }

    virtual ~RooTruncExponential();

    RooArgList exp()
    { return RooArgList(_exp); }

    RooArgList fracs()
    { return RooArgList(_fracs); }

    Int_t getAnalyticalIntegral(RooArgSet &allVars, RooArgSet &analVars,
                                const char *rangeName) const;

    Double_t analyticalIntegral(Int_t code, const char *rangeName) const;

    Int_t getGenerator(const RooArgSet &directVars, RooArgSet &generateVars,
                       Bool_t staticInitOK = kTRUE) const;

    void generateEvent(Int_t code);

    Bool_t ignoreMax()
    { return _ignoreMax; }

    void setIgnoreMax(Bool_t ignore = kTRUE)
    { _ignoreMax = ignore; }

protected:
    RooRealProxy _var;
    RooRealProxy _maxVar;
    RooListProxy _exp;
    RooListProxy _fracs;
    RooRealProxy _lastFrac;
    Bool_t _ignoreMax;

    Double_t evaluate() const;

private:
ClassDef(RooTruncExponential, 1)
};

#endif
