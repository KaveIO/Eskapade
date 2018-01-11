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

#ifndef ROO_ABCD_HIST_PDF
#define ROO_ABCD_HIST_PDF

// esroofit includes.
#include <esroofit/RooParamHistPdf.h>

// RooFit includes.
#include <RooRealProxy.h>
#include <RooCategoryProxy.h>
#include <RooSetProxy.h>
#include <RooListProxy.h>
#include <RooDataHist.h>

// STL includes.
#include <vector>

// RooFit Forward declarations
class RooRealVar;
class RooAbsReal;
class RooDataSet;


namespace Eskapade
{
    namespace ABCD
    {
        Bool_t checkInputData(const RooDataHist &data, Bool_t abcd = kTRUE);
    }
}


class RooABCDHistPdf : public RooParamHistPdf
{
public:
    RooABCDHistPdf(const char *name, const char *title, const RooArgSet &vars,
                   const RooDataHist &dhist, Bool_t doABCD = kTRUE, Bool_t noParams = kTRUE);

    RooABCDHistPdf(const RooABCDHistPdf &other, const char *name = 0);

    virtual TObject *clone(const char *newname) const
    { return new RooABCDHistPdf(*this, newname); }

    virtual ~RooABCDHistPdf();

    inline Bool_t getABCD() const
    { return _abcd; }

    virtual Double_t getSumW() const;

    virtual Double_t getEntriesError(Int_t ibin) const;

    virtual void setNominalData(const RooDataHist &nomHist, Bool_t updateModifiedData = kTRUE);

protected:

    RooListProxy _s;
    RooListProxy _f;

    Bool_t _abcd;
    Int_t _ndim;
    mutable std::vector<double> *_allBCDvec;

    void constructExpectationFunctions();

    virtual void updateModifiedData() const;

    RooAbsReal *createSumEntriesFunc(const char *sumName = "sum_all", const char *cutSpec = "1");

    void calculateData(FastHist *dh_modify_me, Bool_t abcd) const;

    Double_t abcdFunction(const std::vector<Double_t> &bcdVec) const;

private:

ClassDef(RooABCDHistPdf, 1) // Histogram based PDF
};

#endif
