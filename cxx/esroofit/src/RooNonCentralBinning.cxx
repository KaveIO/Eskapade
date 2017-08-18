/*****************************************************************************
 * Project: RooFit                                                           *
 * Package: RooFitCore                                                       *
 * @(#)root/roofitcore:$Id$
 * Authors:                                                                  *
 *   WV, Wouter Verkerke, UC Santa Barbara, verkerke@slac.stanford.edu       *
 *   DK, David Kirkby,    UC Irvine,         dkirkby@uci.edu                 *
 *                                                                           *
 * Copyright (c) 2000-2005, Regents of the University of California          *
 *                          and Stanford University. All rights resized.    *
 *                                                                           *
 * Redistribution and use in source and binary forms,                        *
 * with or without modification, are permitted according to the terms        *
 * listed in LICENSE (http://roofit.sourceforge.net/license.txt)             *
 *****************************************************************************/

//////////////////////////////////////////////////////////////////////////////
//
// BEGIN_HTML
// Class RooNonCentralBinning is an implements RooAbsBinning in terms
// of an array of boundary values, posing no constraints on the choice
// of binning, thus allowing variable bin sizes. Various methods allow
// the user to add single bin boundaries, mirrored pairs, or sets of
// uniformly spaced boundaries.
// END_HTML
//

// esroofit includes.
#include <esroofit/RooNonCentralBinning.h>

// ROOT includes.
#include <Riostream.h>

// STL includes.
#include <algorithm>

using namespace std;


ClassImp(RooNonCentralBinning)


//_____________________________________________________________________________
RooNonCentralBinning::RooNonCentralBinning(Double_t xlo, Double_t xhi, const char *name) :
        RooBinning(xlo, xhi, name)
{
}

//_____________________________________________________________________________
RooNonCentralBinning::RooNonCentralBinning(Int_t nbins, Double_t xlo, Double_t xhi, const char *name) :
        RooBinning(nbins, xlo, xhi, name)
{
    _center.resize(nbins, -1.);
}

//_____________________________________________________________________________
RooNonCentralBinning::RooNonCentralBinning(Int_t nbins, const Double_t *boundaries, const char *name) :
        RooBinning(nbins, boundaries, name)
{
    _center.resize(nbins, -1.);
}

//_____________________________________________________________________________
RooNonCentralBinning::RooNonCentralBinning(const RooNonCentralBinning &other, const char *name) :
        RooBinning(other, name),
        _center(other._center)
{
    // Copy constructor
}

//_____________________________________________________________________________
RooNonCentralBinning::RooNonCentralBinning(const RooBinning &other, const char *name) :
        RooBinning(other, name)
{
    _center.resize(_nbins, -1.);
}

//_____________________________________________________________________________
RooNonCentralBinning::~RooNonCentralBinning()
{
}

//_____________________________________________________________________________
Double_t RooNonCentralBinning::binCenter(Int_t bin) const
{
    //double center = RooBinning::binCenter(bin);

    return (_center[bin] < 0 ? RooBinning::binCenter(bin) : _center[bin]);
}


//_____________________________________________________________________________
void RooNonCentralBinning::setBinCenter(Int_t bin, Double_t value)
{
    if (_nbins > static_cast<int>(_center.size()))
    {
        _center.resize(_nbins, -1.);
    }

    // set the position of the center of bin 'bin'
    if (bin >= 0 && bin < _nbins)
    {
        _center[bin] = value;
    }
}


//_____________________________________________________________________________
void RooNonCentralBinning::setAverageFromData(const RooDataSet &input, const RooRealVar &obs)
{
    const RooArgSet *values = input.get();
    RooRealVar *var = (RooRealVar *) values->find(obs.GetName());

    if (var == NULL)
    { return; }

    if (_nbins != static_cast<int>(_center.size()))
    {
        _center.resize(_nbins, -1.);
    }

    std::vector<Double_t> x0(_nbins, 0.);
    std::vector<Double_t> x1(_nbins, 0.);

    for (Int_t i = 0; i < input.numEntries(); i++)
    {
        input.get(i);
        double w = input.weight();
        double x = var->getVal();

        int bin = this->binNumber(x);
        x0[bin] += w;
        x1[bin] += w * x;
    }

    for (Int_t i = 0; i < _nbins; i++)
    {
        _center[i] = (x0[i] > 0 ? x1[i] / x0[i] : -1.);
    }
}


