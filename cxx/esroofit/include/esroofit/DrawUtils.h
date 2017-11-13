/*****************************************************************************
 * Project: Eskapade - A python-based package for data analysis              *
 * Created: 2017/06/29                                                       *
 * Description:                                                              *
 *     Utility functions for drawing                                         *
 *                                                                           *
 * Authors:                                                                  *
 *     KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands                       *
 *                                                                           *
 * Redistribution and use in source and binary forms, with or without        *
 * modification, are permitted according to the terms listed in the file     *
 * LICENSE.                                                                  *
 *****************************************************************************/

#ifndef ESKAPADE_DRAWUTILS
#define ESKAPADE_DRAWUTILS

// RooFit includes.
#include <RooFitResult.h>

// STL includes.
#include <string>


namespace Eskapade
{
    /**
       Function to plot correlation matrix from RooFitResult
       @param rFit RooFitResult reference containing the correlation matrix
       @param filePath name of output file
    */
    std::string PlotCorrelationMatrix(const RooFitResult &rFit, std::string outDir);
}

#endif
