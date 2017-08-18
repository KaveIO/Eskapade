/*****************************************************************************
 * Project: RooFit                                                           *
 * Package: RooFitCore                                                       *
 *    File: $Id: RooNonCentralBinning.h,v 1.9 2007/05/11 09:11:30 verkerke Exp $
 * Authors:                                                                  *
 *   WV, Wouter Verkerke, UC Santa Barbara, verkerke@slac.stanford.edu       *
 *   DK, David Kirkby,    UC Irvine,         dkirkby@uci.edu                 *
 *                                                                           *
 * Copyright (c) 2000-2005, Regents of the University of California          *
 *                          and Stanford University. All rights reserved.    *
 *                                                                           *
 * Redistribution and use in source and binary forms,                        *
 * with or without modification, are permitted according to the terms        *
 * listed in LICENSE (http://roofit.sourceforge.net/license.txt)             *
 *****************************************************************************/
#ifndef ROO_GRAVITY_BINNING
#define ROO_GRAVITY_BINNING

// ROOT imports.
#include <Rtypes.h>
#include <TList.h>

// RooFit includes.
#include <RooDouble.h>
#include <RooBinning.h>
#include <RooRealVar.h>
#include <RooDataSet.h>
#include <RooNumber.h>

// STL includes.
#include <vector>

// RooFit forward declarations.
class RooAbsPdf;
class RooRealVar;


class RooNonCentralBinning : public RooBinning {
public:

  RooNonCentralBinning(Double_t xlo = -RooNumber::infinity(), Double_t xhi = RooNumber::infinity(), const char* name = 0);
  RooNonCentralBinning(Int_t nBins, Double_t xlo, Double_t xhi, const char* name = 0);
  RooNonCentralBinning(Int_t nBins, const Double_t* boundaries, const char* name = 0);
  RooNonCentralBinning(const RooNonCentralBinning& other, const char* name = 0);
  RooNonCentralBinning(const RooBinning& other, const char* name = 0);

  RooAbsBinning* clone(const char* name = 0) const { return new RooNonCentralBinning(*this,name?name:GetName()); }
  virtual ~RooNonCentralBinning();

  void setAverageFromData(const RooDataSet& input, const RooRealVar& obs);

  virtual void setBinCenter(Int_t bin, Double_t value);
  virtual Double_t binCenter(Int_t bin) const;

protected:

  std::vector<Double_t> _center;   // center of gravity

  ClassDef(RooNonCentralBinning,1) // Generic binning specification
};

#endif
