/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Class  : RooWeibull                                                       *
 * Created: 2017/07/06                                                       *
 * Description:                                                              *
 *     Implementation of Weibull distribution                                *
 *                                                                           *
 *     The Weibull distribution is one of the most widely used lifetime      *
 *     distributions in reliability engineering.  For more details see e.g.: *
 *     http://reliawiki.org/index.php/The_Weibull_Distribution               *
 *                                                                           *
 *     beta  = scale parameter, or characteristic inverse life               *
 *     alpha = shape parameter (or slope)                                    *
 *     gamma = location parameter (or failure free life)                     *
 *                                                                           *
 * Authors:                                                                  *
 *     KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                       *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/


// esroofit includes.
#include <esroofit/RooWeibull.h>

// ROOT includes.
#include "TMath.h"


//_____________________________________________________________________________
RooWeibull::RooWeibull(const char *name, const char *title, RooAbsReal &_x,
                       RooAbsReal &_alpha, RooAbsReal &_beta, RooAbsReal &_gamma) :
        RooAbsPdf(name, title),
        x("x", "x", this, _x),
        alpha("alpha", "alpha", this, _alpha),
        beta("beta", "beta", this, _beta),
        gamma("gamma", "gamma", this, _gamma)
{}

//_____________________________________________________________________________
RooWeibull::RooWeibull(const RooWeibull &other, const char *name) :
        RooAbsPdf(other, name),
        x("x", this, other.x),
        alpha("alpha", this, other.alpha),
        beta("beta", this, other.beta),
        gamma("gamma", this, other.gamma)
{}

//_____________________________________________________________________________
Double_t RooWeibull::evaluate() const
{
    if (x - gamma <= 0)
    { return 0; }
    Double_t r = beta * (x - gamma);
    Double_t f = beta * alpha * TMath::Power(r, alpha - 1.0)
                 * TMath::Exp(-TMath::Power(r, alpha));
    return f;
}

//_____________________________________________________________________________
Int_t RooWeibull::getAnalyticalIntegral(RooArgSet &allVars,
                                        RooArgSet &analVars, const char * /*rangeName*/) const
{
    if (matchArgs(allVars, analVars, x))
    { return 1; }
    return 0;
}

//_____________________________________________________________________________
Double_t RooWeibull::analyticalIntegral(Int_t code, const char *rangeName)
const
{
    switch (code)
    {
        case 1:
        {
            if (x.max(rangeName) - gamma <= 0)
            { return 0; }
            Double_t r_max = beta * (x.max(rangeName) - gamma);
            Double_t r_min = x.min(rangeName) - gamma > 0
                             ? beta * (x.min(rangeName) - gamma) : 0;

            Double_t ret = TMath::Exp(-TMath::Power(r_min, alpha))
                           - TMath::Exp(-TMath::Power(r_max, alpha));
            return ret;
        }
    }

    assert(0);
    return 0;
}
