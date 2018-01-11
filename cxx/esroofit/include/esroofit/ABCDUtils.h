/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Module : ABCDUtils                                                        *
 * Created: 2017/06/03                                                       *
 * Description:                                                              *
 *      Collections of utility functions for RooABCDHistPdf. In particular:
 *      - A function that returns the (significance of the) p-value of the
 *        hypothesis that the observables in the input dataset
 *        are *not* correlated.
 *      - A function returns a dataset with the normalized residuals
          (= pull values) for all bins of the input data.
 *      All functions are implemented as ROOT functions.                     *
 *                                                                           *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                      *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

#ifndef Eskapade_ABCD_Utils
#define Eskapade_ABCD_Utils

// esroofit
#include <esroofit/RooABCDHistPdf.h>
#include <esroofit/RooParamHistPdf.h>

// ROOT includes.
#include <TString.h>

// RooFit includes.
#include <RooDataSet.h>
#include <RooArgSet.h>
#include <RooArgList.h>
#include <RooAbsPdf.h>


namespace Eskapade
{

    namespace ABCD
    {
        // 1.
        // This function returns the (significance of the) p-value of the hypothesis that the observables
        // (obsSet) in the input dataset (dataHist) are *not* correlated.
        //
        // This function first creates a pdf of type RooABCDHistPdf (with setting doABCD = false) from
        // dataHist, and uses that pdf to calculate a chi^2 value wrt input dataHist.
        // This same pdf is then used to simulate nSample datasets (with the same number of entries as dataHist), where to each dataset
        // the same procedure is applied as with the data (i.e. use this as input for RooABCDHistPdf and calculate a chi^2 value).
        // The set of chi^2 values from the simulated samples are fit with a chi^2 distribution to determine the
        // effective number of degrees of freedom (nDof) for this particular setup.
        // The chi^2 value on data, together with nDof, are then used (using the usual chi^2 probability function)
        // to determine the p-value of the hypothesis that the input data is uncorrelated.
        // The function returns the (Gaussian) Z-value of this p-value.
        Double_t SignificanceOfUncorrelatedHypothesis(RooDataHist &dataHist, const RooArgSet &obsSet,
                                                      Int_t nSamples = 500);

        // 2.
        // This function returns a dataset with the normalized residuals (= pull values) for all bins of the
        // input data (datahist) matching the observables in obsSet. This is great for finding significant outliers
        // in data that deviate from your most simple hypothesis (of no correlation).
        // The normalized residuals are obtained by predicting the number of entries for each bin, including an
        // uncertainty on each prediction, using as hypothesis that all observables in obsSet are uncorrelated.
        // Each bin prediction is independent of the number of entries observed in that bin.
        //
        // This function first creates a pdf of type RooABCDHistPdf (with setting doABCD = true) from
        // dataHist, and uses that pdf to make a prediction (including uncertainty) for the number of entries in each bin.
        // The returned dataset includes for each bin:
        // the number of observed entries, the number of predicted entries, the uncertainty on the number of predicted entries,
        // the calculated p-value for that bin, and the Z-value for that bin.
        RooDataSet *GetNormalizedResiduals(const RooDataHist &dataHist, RooArgSet &obsSet,
                                           const char *dataName = 0, RooABCDHistPdf *extPdf = 0);

        // Helper function of 1.
        // genpdf is used to simulate nSamples toy samples with nEvt entries each of the observables in obs.
        // Each simulated sample is used to create a pdf of type RooABCDHistPdf (with setting doABCD, noParams)
        // that is used to calculate a chi^2 value wrt the simulated sample.
        // The set of chi^2 values is returned in a RooDataSet.
        RooDataSet *GenerateAndFit(const RooAbsPdf &genpdf, const RooArgSet &obs, Int_t nSamples,
                                   Int_t nEvt, Bool_t doABCD = kFALSE, Bool_t noParams = kTRUE);

        // Helper function of 2.
        // Function to calculate the error on a given RooABCDHistPdf for each row in the input
        // RooDataSet (data), and add this error as a new column to this dataset (with name errName).
        // obsSet are the observables needed to evaluate the function func.
        // If addPdfVal is true, the function values are stored as a new column as well.
        void AddPropagatedErrorToData(RooDataSet &data, RooArgSet &obsSet, RooABCDHistPdf &pdf,
                                      const char *errName = 0, Bool_t addPdfVal = kFALSE);

        // Helper function of 2.
        // Function to calculate the normalized residual for each row in the input data (data),
        // using the (name of the) observed number of entries (nObsCol), expected (nExpCol), and
        // uncertainty on the expected (nExpErrorCol).
        //
        // n_exp == 0 will always result in p-value=0, even if n_err > 0
        // If nExpZeroCorrection is true, then, when nexp is zero but nexperror is not, nexp is
        // set to nexperror.
        void AddNormResidualToData(RooDataSet &data, TString nObsCol, TString nExpCol,
                                   TString nExpErrorCol, Bool_t nExpZeroCorrection = kFALSE);

        // 3.
        // genpdf is used to simulate nSamples toy samples with nEvt entries each of the observables in obsSet.
        // Each simulated sample is used to create a pdf of type RooABCDHistPdf (with setting doABCD=true, noParams=true)
        // that is used to evaluate the normalized residuals (= pull values) for all bins of the simulated sample.
        // All residuals are collected together in a roodataset and returned to the user.
        RooDataSet *GenerateAndCollectResiduals(const RooAbsPdf &genpdf, RooArgSet &obsSet, Int_t nSamples, Int_t nEvt);

        // 4.
        // Useful for constraint fits, where the RooParamHistPdf or RooABCDHistPdf is built from a MC sample and
        // should be allowed to vary within the MC uncertainties.
        // This function makes a product of RooPoisson pdfs, where each Poisson constrains a bin of the input (MC)
        // dataset of the RooParamHistPdf pdf.
        RooAbsPdf *MakePoissonConstraint(const char *name, RooArgList& storeVarList, RooArgList &storePdfList, const RooParamHistPdf &pdf);

        // Same as above, but binList and nominal data are already taken from the RooParamHistPdf.
        RooAbsPdf *MakePoissonConstraint(const char *name, RooArgList& storeVarList, RooArgList &storePdfList,
                                         const RooArgList &binList, const RooDataHist &nomData);
    }
}

#endif

