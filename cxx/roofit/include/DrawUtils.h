#ifndef Eskapade_DrawUtils
#define Eskapade_DrawUtils

#include "RooFitResult.h"
#include <string>

namespace Eskapade {
  /**
     Function to plot correlation matrix from RooFitResult
     @param rFit RooFitResult reference, from which to get the correlation matrix
     @param filePath name of output file
  */
  std::string PlotCorrelationMatrix(const RooFitResult& rFit, std::string outDir);
}

#endif
