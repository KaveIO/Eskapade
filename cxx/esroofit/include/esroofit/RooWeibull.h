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

#ifndef ROO_WEIBULL
#define ROO_WEIBULL

// RooFit includes.
#include <RooAbsPdf.h>
#include <RooRealProxy.h>
#include <RooCategoryProxy.h>
#include <RooAbsReal.h>
#include <RooAbsCategory.h>
#include <RooRealConstant.h>


class RooWeibull : public RooAbsPdf
{
public:
    RooWeibull()
    {};

    RooWeibull(const char *name, const char *title, RooAbsReal &_x,
               RooAbsReal &_alpha, RooAbsReal &_beta,
               RooAbsReal &_gamma = RooRealConstant::value(0));

    RooWeibull(const RooWeibull &other, const char *name = 0);

    virtual TObject *clone(const char *newname) const
    {
        return new RooWeibull(*this, newname);
    }

    inline virtual ~RooWeibull()
    {}

    Int_t getAnalyticalIntegral(RooArgSet &allVars, RooArgSet &analVars,
                                const char *rangeName = 0) const;

    Double_t analyticalIntegral(Int_t code, const char *rangeName = 0) const;

protected:
    RooRealProxy x;
    RooRealProxy alpha;
    RooRealProxy beta;
    RooRealProxy gamma;

    Double_t evaluate() const;

private:
ClassDef(RooWeibull, 1)
};

#endif
