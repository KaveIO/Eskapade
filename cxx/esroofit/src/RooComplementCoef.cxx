/*****************************************************************************
 * Project: RooFit                                                           *
 *                                                                           *
 * Authors:                                                                  *
 *   JvL, Jeroen van Leerdam, Nikhef, j.van.leerdam@nikhef.nl                *
 *                                                                           *
 * Copyright (c) 2012, Nikhef. All rights reserved.                          *
 *                                                                           *
 * Redistribution and use in source and binary forms,                        *
 * with or without modification, are permitted according to the terms        *
 * listed in LICENSE (http://roofit.sourceforge.net/license.txt)             *
 *****************************************************************************/

// esroofit includes.
#include <esroofit/RooComplementCoef.h>

// ROOT includes.
#include <Riostream.h>

//RooFit includes.
#include <RooMsgService.h>

using std::endl;

//_____________________________________________________________________________
RooComplementCoef::RooComplementCoef(const char *name, const char *title,
                                     const RooArgList &coefficients) :
        RooAbsReal(name, title),
        _coefs("coefficients", "coefficients", this)
{
    // check if coefficients are RooAbsReals
    RooFIter coefIter = coefficients.fwdIterator();
    RooAbsArg *coef = 0;
    while ((coef = coefIter.next()) != 0)
    {
        if (dynamic_cast<RooAbsReal *>(coef) == 0)
        {
            coutE(InputArguments) << "RooComplementCoef::RooComplementCoef("
                                  << GetName() << ") omitting coefficient \"" << coef->GetName()
                                  << "\": not a RooAbsReal" << endl;
            continue;
        }

        _coefs.add(*coef);
    }
}

//_____________________________________________________________________________
RooComplementCoef::RooComplementCoef(
        const RooComplementCoef &other, const char *name) :
        RooAbsReal(other, name),
        _coefs("coefficients", this, other._coefs)
{}

//_____________________________________________________________________________
void RooComplementCoef::printArgs(std::ostream &os) const
{
    os << "[ 1";
    Int_t iter(0);
    RooFIter coefIter(_coefs.fwdIterator());
    RooAbsReal *coef = 0;
    while ((coef = (RooAbsReal *) coefIter.next()) != 0)
    {
        if (iter < 5)
        {
            os << " - " << coef->GetName();
            ++iter;
        }
        else
        {
            os << " - ...";
            break;
        }
    }
    os << " (" << _coefs.getSize() << " coefficients) ]";
}

//_____________________________________________________________________________
Double_t RooComplementCoef::evaluate() const
{
    Double_t result = 1.;
    RooFIter coefIter(_coefs.fwdIterator());
    RooAbsReal *coef = 0;
    while ((coef = (RooAbsReal *) coefIter.next()) != 0) result -= coef->getVal();
    return result;
}
