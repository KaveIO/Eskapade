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

#ifndef Eskapade_Data_Utils
#define Eskapade_Data_Utils

// RooFit includes.
#include <RooDataHist.h>
#include <RooDataSet.h>
#include <RooAbsReal.h>
#include <RooFitResult.h>


namespace Eskapade
{

    // Function to convert a roodatahist into a roodataset.
    // Required input is the roodatahist (dataHist). Optionally provide a new name (dsName).
    RooDataSet *ConvertDataHistToDataSet(const RooDataHist &dataHist, const char *dsName = 0);

    // Function to calculate the error on a given function for each row in the input
    // RooDataSet (data), and add this error as a new column to this dataset (with name errName).
    // obsSet are the observables needed to evaluate the function func.
    // fitResult is needed to do error propagation on each func value.
    // If addFuncVal is true, the function values are stored as a new column as well.
    void AddPropagatedErrorToData(RooDataSet &data, RooArgSet &obsSet, RooAbsReal &func,
                                  const RooFitResult &fitResult,
                                  const char *errName = 0, Bool_t addFuncVal = kFALSE);
}

#endif
