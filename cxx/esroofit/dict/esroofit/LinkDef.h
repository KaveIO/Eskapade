#ifdef __CLING__

#pragma link off all globals;
#pragma link off all classes;
#pragma link off all functions;

#pragma link C++ namespace Eskapade;
#pragma link C++ namespace Eskapade::ABCD;

#pragma link C++ class TMsgLogger;

#pragma link C++ class RooComplementCoef+;
#pragma link C++ class RooTruncExponential+;
#pragma link C++ class RooParamHistPdf+;
#pragma link C++ class RooABCDHistPdf+;
#pragma link C++ class RooExpandedFitResult+;
#pragma link C++ class RooWeibull+;
#pragma link C++ class RooNonCentralBinning+;
#pragma link C++ class RhhNDKeysPdf+;

#pragma link C++ function Eskapade::PoissonObsP(Double_t,Double_t,Double_t);
#pragma link C++ function Eskapade::PoissonObsMidP(Double_t,Double_t,Double_t);
#pragma link C++ function Eskapade::PoissonObsZ(Double_t,Double_t,Double_t);
#pragma link C++ function Eskapade::PoissonObsMidZ(Double_t,Double_t,Double_t);
#pragma link C++ function Eskapade::PoissonNormalizedResidual(Double_t,Double_t,Double_t);

#endif
