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

// esroofit includes.
#include <esroofit/Statistics.h>

// ROOT includes.
#include <TVectorD.h>

// RooFit includes.
#include <RooRealVar.h>

// RooStats includes.
#include <RooStats/NumberCountingUtils.h>
#include <RooStats/RooStatsUtils.h>

using namespace RooStats;
using namespace std;

/*
 * Adopted from: RooAbsReal::GetPropagatedError()
 * by Wouter Verkerke
 * See: http://root.cern.ch/root/html534/src/RooAbsReal.h.html
 * (http://roofit.sourceforge.net/license.txt)
 */

//_____________________________________________________________________________
Double_t Eskapade::GetPropagatedError(RooAbsReal& var, const RooFitResult& fr, const bool& doAverageAsymErrors)
{
    RooArgSet* errorParams = var.getObservables(fr.floatParsFinal()) ;
    RooArgSet* nset = var.getParameters(*errorParams) ;

    RooArgList paramList ;
    const RooArgList& fpf = fr.floatParsFinal() ;
    vector<int> fpf_idx ;
    for (Int_t i=0 ; i<fpf.getSize() ; i++) {
        RooAbsArg* par = errorParams->find(fpf[i].GetName()) ;
        if (par ) {
            if (! par->isConstant() ) {
                paramList.add(*par) ;
                fpf_idx.push_back(i) ;
            }
        }
    }

    vector<Double_t> plusVar, minusVar ;

    TMatrixDSym V( fr.covarianceMatrix() ) ;

    for (Int_t ivar=0 ; ivar<paramList.getSize() ; ivar++) {

        RooRealVar& rrv = (RooRealVar&)fpf[fpf_idx[ivar]] ;

        int newI = fpf_idx[ivar];

        Double_t cenVal = rrv.getVal() ;
        Double_t errHes = sqrt(V(newI,newI)) ;

        Double_t errHi = rrv.getErrorHi();
        Double_t errLo = TMath::Abs( rrv.getErrorLo() );

        if (!doAverageAsymErrors) {
          errHi = errHes;
          errLo = errHes;
        }

        // Make Plus variation
        ((RooRealVar*)paramList.at(ivar))->setVal(cenVal+errHi) ;
        plusVar.push_back(var.getVal(nset)) ;

        // Make Minus variation
        ((RooRealVar*)paramList.at(ivar))->setVal(cenVal-errLo) ;
        minusVar.push_back(var.getVal(nset)) ;

        ((RooRealVar*)paramList.at(ivar))->setVal(cenVal) ;

    }

    TMatrixDSym C(paramList.getSize()) ;
    vector<double> errVec(paramList.getSize()) ;
    for (int i=0 ; i<paramList.getSize() ; i++) {
        int newII = fpf_idx[i];
        errVec[i] = sqrt(V(newII,newII)) ;
        for (int j=i ; j<paramList.getSize() ; j++) {
            int newJ = fpf_idx[j];
            C(i,j) = V(newII,newJ)/sqrt(V(newII,newII)*V(newJ,newJ)) ;
            C(j,i) = C(i,j) ;
        }
    }

    // Make vector of variations
    TVectorD F(plusVar.size()) ;

    for (unsigned int j=0 ; j<plusVar.size() ; j++) {
        F[j] = (plusVar[j]-minusVar[j])/2 ;
    }

    // Calculate error in linear approximation from variations and correlation coefficient
    Double_t sum = F*(C*F) ;

    return sqrt(sum) ;
}


Double_t
Eskapade::PoissonObsP(Double_t mainObs, Double_t backgroundObs, Double_t relativeBkgUncert)
{
  Double_t p(0.);

  if (relativeBkgUncert > 0) {
    // See for details: https://root.cern.ch/root/html526/src/RooStats__NumberCountingUtils.h.html#100
    // However, correct for bug in case mainObs equals 0 records, in which case BinomialObsP returns 0, which should be 1.
    p = (mainObs != 0 ? NumberCountingUtils::BinomialObsP(mainObs, backgroundObs, relativeBkgUncert) : 1);
  } else {
    p = (mainObs !=0 ? 1.-ROOT::Math::poisson_cdf(mainObs-1, backgroundObs) : 1);
  }

  return p;
}


Double_t
Eskapade::PoissonObsMidP(Double_t mainObs, Double_t backgroundObs, Double_t relativeBkgUncert)
{
  Double_t p = Eskapade::PoissonObsP(mainObs, backgroundObs, relativeBkgUncert);

  // Converting from discrete (Poisson) to continuous interpretation (Gaussian),
  // to do so, we are applying the Lancaster mid-p correction
  // E.g. see for details: http://www.stat.ufl.edu/~aa/articles/agresti_gottard_2005.pdf
  Double_t mid_p = 0.5 * TMath::Poisson(mainObs,backgroundObs);
  p -= mid_p;

  return p;
}


Double_t
Eskapade::PoissonObsZ(Double_t mainObs, Double_t backgroundObs, Double_t relativeBkgUncert)
{
  // convert p-value to significance
  return RooStats::PValueToSignificance( Eskapade::PoissonObsP(mainObs, backgroundObs, relativeBkgUncert) );
}


Double_t
Eskapade::PoissonObsMidZ(Double_t mainObs, Double_t backgroundObs, Double_t relativeBkgUncert)
{
  // converting to significance
  return RooStats::PValueToSignificance( Eskapade::PoissonObsMidP(mainObs, backgroundObs, relativeBkgUncert) );
}


Double_t
Eskapade::PoissonNormalizedResidual(Double_t mainObs, Double_t backgroundObs, Double_t relativeBkgUncert)
{
  return Eskapade::PoissonObsMidZ(mainObs, backgroundObs, relativeBkgUncert);
}
