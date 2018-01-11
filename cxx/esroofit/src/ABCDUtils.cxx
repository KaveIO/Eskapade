/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Module : ABCDUtils                                                        *
 * Created: 2017/06/03                                                       *
 * Description:                                                              *
 *      Collections of utility functions for RooABCDHistPdf. In particular:  *
 *      - A function that returns the (significance of the) p-value of the   *
 *        hypothesis that the observables in the input dataset               *
 *        are *not* correlated.                                              *
 *      - A function returns a dataset with the normalized residuals         *
 *        (= pull values) for all bins of the input data.                    *
 *      All functions are implemented as ROOT functions.                     *
 *                                                                           *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

// esroofit includes.
#include <esroofit/ABCDUtils.h>
#include <esroofit/Statistics.h>
#include <esroofit/DataUtils.h>
#include <esroofit/TMsgLogger.h>

// ROOT includes.
#include <Math/SpecFuncMathCore.h>

// RooFit includes.
#include <RooPoisson.h>
#include <RooProdPdf.h>
#include <RooRealVar.h>
#include <RooChiSquarePdf.h>

// RooStats includes.
#include <RooStats/RooStatsUtils.h>

namespace Eskapade
{
    namespace ABCD
    {
        static TMsgLogger logger("Eskapade::ABCDUtils");
    }
}


////////////////////////////////////////////////////////////////////////////////
RooDataSet *
Eskapade::ABCD::GenerateAndFit(const RooAbsPdf &genpdf, const RooArgSet &obs, Int_t nSamples,
                               Int_t nEvt, Bool_t doABCD, Bool_t noParams)
{
    R__ASSERT(nSamples >= 0);
    R__ASSERT(nEvt >= 1);

    RooRealVar chi2("chi2", "chi2", 0);
    RooArgSet chi2set(chi2);

    //RooAbsData::setDefaultStorageType(RooAbsData::Tree);
    const char *cname = Form("chi2data_%s_%d_%d", genpdf.GetName(), nSamples, nEvt);
    RooDataSet *chi2data = new RooDataSet(cname, cname, chi2set);

    RooABCDHistPdf *pdf(0);
    RooDataHist *initData(0);

    if (!noParams)
    {
        initData = genpdf.generateBinned(obs, nEvt);
        const char *pname = Form("%s_%d", genpdf.GetName(), 0);
        pdf = new RooABCDHistPdf(pname, pname, obs, *initData, doABCD, noParams);
    }

    for (Int_t i = 0; i < nSamples; i++)
    {

        if (i % 100 == 0)
        {
            logger << kDEBUG << "Generating and processing sample: " << i << " / " << nSamples << GEndl;
        }

        RooDataHist *dhi = genpdf.generateBinned(obs, nEvt);
        const char *pname = Form("%s_%d", genpdf.GetName(), i);

        // Note: the construction of the abcd pdf encapsulates the fit.
        if (noParams)
        {
            pdf = new RooABCDHistPdf(pname, pname, obs, *dhi, doABCD, noParams);
        }
        else
        {
            pdf->setNominalData(*dhi);
        }

        RooAbsReal *chi2vari = pdf->createChi2(*dhi);
        chi2.setVal(chi2vari->getVal());
        chi2data->add(chi2set);

        delete dhi;
        if (noParams)
        { delete pdf; }
        delete chi2vari;
    }

    if (!noParams)
    {
        delete initData;
        delete pdf;
    }

    return chi2data;
}


RooAbsPdf*
Eskapade::ABCD::MakePoissonConstraint(const char* name, RooArgList& storeVarList, RooArgList& storePdfList,
                                      const RooParamHistPdf& pdf)
{
  const RooDataHist& data = pdf.getInitialData();
  Bool_t relParams = pdf.getRelParams();
  const RooArgList& binList = relParams ? pdf.binList() : pdf.paramList();
  return Eskapade::ABCD::MakePoissonConstraint(name, storeVarList, storePdfList, binList, data);
}


RooAbsPdf*
Eskapade::ABCD::MakePoissonConstraint(const char* name, RooArgList& storeVarList, RooArgList& storePdfList,
                                      const RooArgList& binList, const RooDataHist& nomData)
{
  R__ASSERT(binList.getSize()==nomData.numEntries());

  for (Int_t i=0; i<binList.getSize(); i++) {
    // expectation
    RooAbsReal& bini = (RooAbsReal&)binList[i];
    // observation
    nomData.get(i);
    Double_t w = nomData.weight();
    const char* nnamei = Form("%s_nominal_%d", name, i);
    RooRealVar* nomi = new RooRealVar(nnamei,nnamei,w,w,w);
    nomi->setConstant();
    storeVarList.addOwned(*nomi);
    // construct poissons
    const char* pnamei = Form("%s_%d", name, i);
    RooPoisson* poisi = new RooPoisson(pnamei,pnamei,*nomi,bini);
    storePdfList.addOwned(*poisi);
  }

  RooProdPdf* prod(0);
  if (storePdfList.getSize()>0)
    prod = new RooProdPdf(name,name,storePdfList);
  return prod;
}


void
Eskapade::ABCD::AddNormResidualToData(RooDataSet &data, TString nObsCol, TString nExpCol,
                                      TString nExpErrorCol, Bool_t nExpZeroCorrection)
{
    R__ASSERT(!nObsCol.IsNull());
    R__ASSERT(!nExpCol.IsNull());
    R__ASSERT(!nExpErrorCol.IsNull());

    const RooArgSet *vars = data.get();
    RooRealVar *nObs = (RooRealVar *) vars->find(nObsCol.Data());
    RooRealVar *nExp = (RooRealVar *) vars->find(nExpCol.Data());
    RooRealVar *nErr = (RooRealVar *) vars->find(nExpErrorCol.Data());

    R__ASSERT(nObs != 0);
    R__ASSERT(nExp != 0);
    R__ASSERT(nErr != 0);

    RooRealVar z("normResid", "normResid", 0);
    RooRealVar pv("pValue", "pValue", 0);
    RooArgSet zset(z, pv);
    RooDataSet *zdata = new RooDataSet("zdata", "zdata", zset);

    for (Int_t i = 0; i < data.numEntries(); i++)
    {
        data.get(i); // update vars

        if (i % 100 == 0)
        {
            logger << kDEBUG << "Adding normalized residual to bin " << i << " of data set " << data.GetName() << GEndl;
        }

        Double_t n_obs = nObs->getVal();
        Double_t n_exp = nExp->getVal();
        Double_t n_err = nErr->getVal();
        R__ASSERT(n_obs >= 0);
        R__ASSERT(n_exp >= 0);
        R__ASSERT(n_err >= 0);

        Double_t rel_error(n_exp > 0 ? n_err / n_exp : 1);
        Double_t n_exp_alt(n_exp);

        // n_exp == 0 will always result in pval=0, even if n_err > 0
        // alternative scenario: reset nexp to nerr when this happens.
        // below will use this correction by default, unless the flag nExpZeroCorrection
        // is set to false.
        if (n_exp == 0)
        {
            if (n_obs > 0)
            {
                n_exp_alt = n_err;
            }
        }

        Double_t p = Eskapade::PoissonObsMidP(n_obs, nExpZeroCorrection ? n_exp_alt : n_exp, rel_error);
        Double_t Z = Eskapade::PoissonObsMidZ(n_obs, nExpZeroCorrection ? n_exp_alt : n_exp, rel_error);

        pv.setVal(p);
        z.setVal(Z);
        zdata->add(zset);
    }

    data.merge(zdata);
    delete zdata;
}


RooDataSet *
Eskapade::ABCD::GetNormalizedResiduals(const RooDataHist &dataHist, RooArgSet &obsSet, const char *dataName,
                                       RooABCDHistPdf *extPdf)
{
    RooABCDHistPdf *pdf(0);
    if (extPdf == 0)
    {
        pdf = new RooABCDHistPdf("abcd", "abcd", obsSet, dataHist, true, true); // abcd on, params off
    }
    else
    {
        pdf = extPdf;
    }

    RooDataSet *dataSet = Eskapade::ConvertDataHistToDataSet(dataHist, dataName);
    // add pdf values
    pdf->setUnitNorm(true);
    dataSet->addColumn(*pdf);

    // add pdf errors
    const char *errName = "abcd_error";
    Eskapade::ABCD::AddPropagatedErrorToData(*dataSet, obsSet, *pdf, errName);

    // add residuals
    const char *valName = pdf->GetName();
    Bool_t nExpZeroCorrection = kTRUE;
    Eskapade::ABCD::AddNormResidualToData(*dataSet, "num_entries", valName, errName, nExpZeroCorrection);

    if (extPdf == 0)
    {
        delete pdf;
    }
    else
    { // reset
        pdf->setUnitNorm(false);
    }

    return dataSet; // user owns dataSet
}


RooDataSet *
Eskapade::ABCD::GenerateAndCollectResiduals(const RooAbsPdf &genpdf, RooArgSet &obsSet, Int_t nSamples,
                                            Int_t nEvt)
{
    R__ASSERT(nSamples >= 0);
    R__ASSERT(nEvt >= 1);

    //RooDataHist* initHist = genpdf.generateBinned(obsSet, nEvt);
    //RooABCDHistPdf* extpdf = new RooABCDHistPdf("abcd", "abcd", obsSet, *initHist);
    //extpdf->setAllGammaConstant(false);

    RooDataSet *residCollection(0);

    for (Int_t i = 0; i < nSamples; i++)
    {
        if (i % 10 == 0)
        {
            logger << kDEBUG << "Generating and processing sample: " << i << " / " << nSamples << GEndl;
        }

        RooDataHist *dhi = genpdf.generateBinned(obsSet, nEvt);

        const char *dname = Form("resid_%d", i);
        RooDataSet *residi = Eskapade::ABCD::GetNormalizedResiduals(*dhi, obsSet, dname);
        //RooDataSet* residi = Eskapade::ABCD::GetNormalizedResiduals(*dhi, obsSet, dname, extpdf);

        // append to residual collection
        if (i == 0)
        {
            residCollection = (RooDataSet *) residi->Clone("residual_collection");
            residCollection->reset();
        }
        residCollection->append(*residi);

        delete dhi;
        delete residi;
    }

    residCollection->SetTitle(residCollection->GetName());
    return residCollection;
}


Double_t
Eskapade::ABCD::SignificanceOfUncorrelatedHypothesis(RooDataHist &dataHist, const RooArgSet &obsSet,
                                                     Int_t nSamples)
{
    Int_t nEvt = (Int_t) dataHist.sumEntries();

    R__ASSERT(nSamples >= 0);
    R__ASSERT(nEvt >= 1);

    // turn off all roofit messages for now
    RooMsgService::instance().saveState();
    RooMsgService::instance().setGlobalKillBelow(RooFit::FATAL); // kill all below fatal

    RooABCDHistPdf pdf("abcd", "abcd", obsSet, dataHist, false, true); // no abcd, no parameters (fast!)
    RooDataSet *chi2data = Eskapade::ABCD::GenerateAndFit(pdf, obsSet, nSamples, nEvt, false, true);

    const RooArgSet *chi2set = chi2data->get();
    RooRealVar *chi2 = (RooRealVar *) chi2set->find("chi2");

    Double_t lowest(0), highest(0);
    chi2data->getRange(*chi2, lowest, highest);
    Double_t ndof_mean = chi2data->mean(*chi2);
    RooRealVar ndof("ndof", "ndof", ndof_mean, lowest, highest);

    RooChiSquarePdf chi2pdf("chi2pdf", "chi2pdf", *chi2, ndof);
    // do this fit to determine best value of ndof
    RooFitResult *fit_result = chi2pdf.fitTo(*chi2data, RooFit::PrintLevel(-1), RooFit::Save());
    Double_t ndof_best(ndof_mean);
    if (fit_result->status() == 0)
    {
        ndof_best = ndof.getVal();
    }

    // chi2 value of original dataset
    RooAbsReal *chi2var = pdf.createChi2(dataHist);
    Double_t chi2_value_data = chi2var->getVal();

    Double_t prob = ROOT::Math::inc_gamma_c(ndof_best / 2., chi2_value_data / 2.);

    delete chi2data;
    delete chi2var;

    RooMsgService::instance().restoreState();

    return RooStats::PValueToSignificance(prob);
}


void Eskapade::ABCD::AddPropagatedErrorToData(RooDataSet &data, RooArgSet &obsSet, RooABCDHistPdf &pdf,
                                              const char *errName, Bool_t addPdfVal)
{
    Bool_t abcd = pdf.getABCD();
    R__ASSERT(abcd);

    TIterator *obsItr = obsSet.createIterator();
    const RooArgSet *vars = data.get();

    // check that all required observables exist in dataset
    RooAbsArg *obs;
    for (Int_t j = 0; (obs = (RooAbsArg *) obsItr->Next()); ++j)
    {
        RooAbsArg *var = vars->find(obs->GetName());
        R__ASSERT(var != 0);
    }
    delete obsItr;

    const char *peName = (errName != 0 ? errName : Form("%s_error", pdf.GetName()));
    RooRealVar perror(peName, peName, 0);
    RooArgSet perror_set(perror);
    RooDataSet *perror_data = new RooDataSet("perror_data", "perror_data", perror_set);

    for (Int_t i = 0; i < data.numEntries(); i++)
    {
        if (i % 10 == 0)
        {
            logger << kDEBUG << "Adding propagated error to bin " << i << " of data set " << data.GetName() << GEndl;
        }
        data.get(i); // update vars
        Int_t jdx = pdf.getIndex(*vars);
        Double_t errorVal = pdf.getEntriesError(jdx);
        perror.setVal(errorVal);
        perror_data->add(perror_set);
    }

    // merge function values and errors into data
    if (addPdfVal)
    {
        pdf.setUnitNorm(true);
        data.addColumn(pdf);
        pdf.setUnitNorm(false);
    }
    data.merge(perror_data);
    delete perror_data;
}
