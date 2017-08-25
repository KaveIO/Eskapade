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

#ifndef ROO_COMPLEMENT_COEF
#define ROO_COMPLEMENT_COEF

#include <RooAbsReal.h>
#include <RooListProxy.h>

// RooFit forward declarations.
class RooArgList;


class RooComplementCoef : public RooAbsReal
{

public:
    RooComplementCoef()
    {};

    // constructor
    RooComplementCoef(const char *name, const char *title, const RooArgList &coefficients);

    // copy constructor
    RooComplementCoef(const RooComplementCoef &other, const char *name = 0);

    virtual TObject *clone(const char *name) const
    {
        return new RooComplementCoef(*this, name);
    }

    inline virtual ~RooComplementCoef()
    {}

    RooArgList coefficients() const
    { return RooArgList(_coefs, "coefficients"); }

    void printArgs(std::ostream &os) const;

protected:
    RooListProxy _coefs;

    Double_t evaluate() const;

private:
ClassDef(RooComplementCoef, 1)
};

#endif
