/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Class  : RooParamHistPdf                                                  *
 * Created: 2017/06/03                                                       *
 * Description:                                                              *
 *      Pdf class derived from RooHistPdf which multiplies each bin
 *      with a RooRealVar scale parameter, e.g. which can be fit or
 *      constrained.
 *
 *      By default these parameters are normalized wrt the bin count,
 *      except when the number of entries in a bin is zero, in which case
 *      the parameter is the number of entries in the bin.
 *      Relative or absolute scale parameters are set with the flag: 
 *      relParams (default is false).
 *      Request these parameters with the function: paramList()
 *      The generation of the scale parameters can be turned off with the
 *      flag: noParams (default is false).
 *
 *      RooParamHistPdf works internally with another RooDataHist
 *      (modified for speedup), which gets updated whenever a parameter      *
 *      value is changed. Several RooHistPdf functions have been overloaded  *
 *      to pick up this modified roodatahist object instead.                 *
 *      This internal roodatahist can be updated at any time with the        *
 *      function setModifiedData(), which requires noParams = false.         *
 *                                                                           *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

#ifndef ROO_PARAM_HIST_PDF
#define ROO_PARAM_HIST_PDF

// RooFit includes.
#include <RooHistPdf.h>
#include <RooRealProxy.h>
#include <RooCategoryProxy.h>
#include <RooSetProxy.h>
#include <RooListProxy.h>
#include <RooDataHist.h>

// RooFit forward declarations.
class RooRealVar;
class RooAbsReal;


class RooParamHistPdf : public RooHistPdf
{
public:
    RooParamHistPdf(const char *name, const char *title, const RooArgSet &vars, const RooDataHist &dhist,
                    Int_t intOrder = 0, Bool_t noParams = kFALSE, Bool_t relParams=kFALSE);

    RooParamHistPdf(const RooParamHistPdf &other, const char *name = 0);

    virtual TObject *clone(const char *newname) const
    { return new RooParamHistPdf(*this, newname); }

    virtual ~RooParamHistPdf();

    Double_t analyticalIntegral(Int_t code, const char *rangeName = 0) const;

    virtual Double_t maxVal(Int_t code) const;

    const RooDataHist &getInitialData() const
    { return _dh; }

    const RooDataHist &getModifiedData() const;

    const RooArgList &paramList() const
    { return _p; }

    const RooArgList &nomList() const
    { return _c; }

    const RooArgList &binList() const
    { return _b; }

    void setAllGammaConstant(Bool_t constant = kTRUE);

    void setGammaConstant(Int_t i, Bool_t constant = kTRUE);

    Bool_t hasTrackerChanged() const;

    Double_t getEntries(Int_t ibin);

    virtual Double_t getEntriesError(Int_t ibin) const;

    Double_t getNominal(Int_t ibin) const;

    Double_t getNominalError(Int_t ibin) const;

    virtual void setNominalData(const RooDataHist &nomHist, Bool_t updateModifiedData = kTRUE);

    inline Bool_t getNoParams() const
    { return _noParams; }

    inline Bool_t getRelParams() const
    { return _relParams; }

    inline Double_t getSumWNorm() const
    { return _sumWnorm; }

    virtual Double_t getSumW() const;

    Int_t getIndex(const RooArgSet &coord, Bool_t fast = kFALSE)
    { return _dh_mod->getIndex(coord, fast); }

protected:

    class FastHist : public RooDataHist
    {
    public:
        FastHist(const FastHist &other, const char *name = 0) : RooDataHist(other, name)
        {}

        FastHist(const RooDataHist &other, const char *name = 0) : RooDataHist(other, name)
        {}

        virtual ~FastHist()
        {};

        void setWeight(Int_t i, Double_t w)
        { _wgt[i] = w; }

        void setSumW2(Int_t i, Double_t sw2)
        { _sumw2[i] = sw2; }
    };

    RooListProxy _p; // scale parameters, can be fit or constrained.
    RooListProxy _c; // original nominal bin entries
    RooListProxy _b; // new bin entries: b = p * c
    RooListProxy _t; // change tracker of parameters in _p

    const RooDataHist &_dh; //! do not persist
    mutable FastHist *_dh_mod; //! do not persist
    Bool_t _noParams;
    Bool_t _relParams;
    Double_t _sumWnorm;

    Double_t evaluate() const;

    virtual void updateModifiedData() const;

    void createBinParameters();

private:

ClassDef(RooParamHistPdf, 1) // Histogram based PDF
};

#endif
