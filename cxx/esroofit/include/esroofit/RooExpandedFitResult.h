/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Class  : RooExpandedFitResult                                             *
 * Created: March 2012
 *
 * Description:                                                              *
 *      Class derived from RooFitResult, to be able to:
 *      - add more parameters for error propagation
 *        (for calculation & visualization purposes)                         *
 *      - Build a RooFitResult object from set of parameters
 *                                                                           *
 * Authors:                                                                  *
 *      HistFitter group, CERN, Geneva                                       *
 *
 * Copyright:
 *      HistFitter
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

#ifndef ROOEXPANDEDFITRESULT_H
#define ROOEXPANDEDFITRESULT_H

// ROOT includes.
#include <TString.h>

// RooFit includes
#include <RooFitResult.h>
#include <RooArgList.h>

// STL includes.
#include <iostream>
#include <vector>


class RooExpandedFitResult : public RooFitResult
{

public:
    RooExpandedFitResult(const RooFitResult &origResult, const RooArgList &extraPars);

    RooExpandedFitResult(const RooArgList &extraPars);

    ~RooExpandedFitResult()
    {}

ClassDef(RooExpandedFitResult, 1) // Container class for expanded fit result
};

#endif
