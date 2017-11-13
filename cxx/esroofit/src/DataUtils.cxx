/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Module : DataUtils                                                        *
 * Created: 2017/06/03                                                       *
 * Description:                                                              *
 *      Collections of utility functions for processing RooFit RooDataSets:
 *      - convert a roodatahist into a roodataset
 *      - calculate the error on a given function for each row in a
 *        RooDataSet, and add this error as a new column to the dataset.
 *      Both functions are implemented as ROOT functions.                    *
 *                                                                           *
 * Authors:                                                                  *
 *      KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                      *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

// esroofit includes.
#include <esroofit/DataUtils.h>
#include <esroofit/Statistics.h>
#include <esroofit/TMsgLogger.h>

// RooFit includes.
#include <RooRealVar.h>
#include <RooCategory.h>


namespace Eskapade
{
    static TMsgLogger logger("Eskapade.DataUtils");
}

RooDataSet *
Eskapade::ConvertDataHistToDataSet(const RooDataHist &dataHist, const char *dsName)
{
    RooArgSet obsw_set("obsw_set");
    RooRealVar num_entries("num_entries", "num_entries", 0);
    RooRealVar weight("weight", "weight", 0);
    RooRealVar weight_error("weight_error", "weight_error", 0);

    // roodataset will get weight variable
    const RooArgSet *obs_set = dataHist.get();
    obsw_set.add(*obs_set);
    obsw_set.add(num_entries);
    obsw_set.add(weight_error);
    obsw_set.add(weight);

    // create roodataset, make sure it can be easily converted to a tree later on.
    const char *name = (dsName != 0 ? dsName : Form("rds_%s", dataHist.GetName()));

    RooDataSet *rds = new RooDataSet(name, name, obsw_set, weight.GetName());

    for (Int_t i = 0; i < dataHist.numEntries(); i++)
    {
        dataHist.get(i); // retrieve info for record i
        num_entries.setVal(dataHist.weight());
        weight_error.setVal(dataHist.weightError());
        weight.setVal(dataHist.weight());
        rds->add(obsw_set, dataHist.weight());
    }

    return rds;
}


void
Eskapade::AddPropagatedErrorToData(RooDataSet &data, RooArgSet &obsSet, RooAbsReal &func,
                                   const RooFitResult &fitResult,
                                   const char *errName, Bool_t addFuncVal)
{
    TIterator *obsItr = obsSet.createIterator();
    const RooArgSet *vars = data.get();

    // check that all required observables exist in dataset
    RooAbsArg *obs;
    for (Int_t j = 0; (obs = (RooAbsArg *) obsItr->Next()); ++j)
    {
        RooAbsArg *var = vars->find(obs->GetName());
        R__ASSERT(var != 0);
    }

    const char *peName = (errName != 0 ? errName : Form("%s_error", func.GetName()));
    RooRealVar perror(peName, peName, 0);
    RooArgSet perror_set(perror);
    RooDataSet *perror_data = new RooDataSet("perror_data", "perror_data", perror_set);

    for (Int_t i = 0; i < data.numEntries(); i++)
    {
        data.get(i); // update vars

        if (i % 10 == 0)
        {
            logger << kDEBUG << "Adding propagated error to bin " << i << " of data set " << data.GetName() << GEndl;
        }

        // update observable values
        obsSet = *vars;

        //Double_t errorVal = func.getPropagatedError( fitResult );
        Double_t errorVal = Eskapade::GetPropagatedError(func, fitResult, true); // pick up asym errors
        perror.setVal(errorVal);
        perror_data->add(perror_set);
    }

    delete obsItr;

    // merge function values and errors into data
    if (addFuncVal)
    {
        data.addColumn(func);
    }
    data.merge(perror_data);
    delete perror_data;
}

