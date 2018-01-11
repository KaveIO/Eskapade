/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Module : Statistics                                                       *
 * Created: 2017/06/03                                                       *
 * Description:                                                              *
 *      Collections of statistics utility functions, based on RooStats       *
 *      and RooFit:                                                          *
 *      - number counting utilities, giving the corresponding p and Z values *
 *      - error propagation function.                                        *
 *      The functions are implemented as ROOT functions.                     *
 *                                                                           *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                      *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

#ifndef Eskapade_Statistics
#define Eskapade_Statistics

// ROOT includes.
#include <Rtypes.h>

// RooFit includes.
#include <RooAbsReal.h>
#include <RooFitResult.h>


namespace Eskapade
{
    /**
       Function to calculate the propagated error of given RooAbsReal object

       Modification of: https://root.cern.ch/doc/master/RooAbsReal_8cxx_source.html#l02636
       to process asymmetric input uncertainties.

       @param var RooAbsReal reference to the object that the error gets calculated for
       @param fr const RooFitResult reference used contaning the fit result and covariance matrix used for error propagation
       @param doAverageAsymErrors Boolean deciding whether asymmetric (MINOS) errors on parameters get used in an averaged approximation
       @return Returns the symmetrized propagated error
    */
    Double_t GetPropagatedError(RooAbsReal &var, const RooFitResult &fr, const bool &doAverageAsymErrors = false);


    // --- Number counting utility functions

    // Based on RooStats::NumberCountingUtils::BinomialObsP
    // and on: ROOT::Math::poisson_cdf
    // See: https://root.cern.ch/root/html526/src/RooStats__NumberCountingUtils.h.html#100
    // and: https://root.cern.ch/doc/v608/ProbFuncMathCore_8cxx_source.html#l00284
    Double_t PoissonObsP(Double_t nObs, Double_t nExp, Double_t fractionalBUncertainty = 0);

    // Mid-P-Value calculation, to adjust for discreteness in small-sample distributions
    // For details on mid p-value, search on "Lancaster mid p-value correction"
    Double_t PoissonObsMidP(Double_t nObs, Double_t nExp, Double_t fractionalBUncertainty = 0);

    // PoissonObsMidZ returns the normalized residual (aka pull) when observing Nobs events,
    // and expecting Nexp with a relative uncertainty of fractionalBUncertainty
    Double_t PoissonObsZ(Double_t nObs, Double_t nExp, Double_t fractionalBUncertainty = 0);

    // significance from mid-P-Value calculation, to adjust for discreteness in small-sample distributions
    Double_t PoissonObsMidZ(Double_t nObs, Double_t nExp, Double_t fractionalBUncertainty = 0);

    // same as PoissonObsMidZ
    Double_t PoissonNormalizedResidual(Double_t nObs, Double_t nExp, Double_t fractionalBUncertainty = 0);
}

#endif

