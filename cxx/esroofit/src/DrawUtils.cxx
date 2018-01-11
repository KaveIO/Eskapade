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

// esroofit includes.
#include <esroofit/DrawUtils.h>

// ROOT includes.
#include <TCanvas.h>
#include <TH2D.h>
#include <TStyle.h>
#include <TROOT.h>


std::string Eskapade::PlotCorrelationMatrix(const RooFitResult &rFit,
                                            std::string outDir)
{
    std::string canvName = Form("correlation_matrix_%s", rFit.GetName());
    TCanvas c_corr(canvName.c_str(), canvName.c_str(), 1200, 800); // .c_str())

    Double_t orig_MarkerSize = gStyle->GetMarkerSize();
    Int_t orig_MarkerColor = gStyle->GetMarkerColor();
    const char *orig_PaintTextFormat = gStyle->GetPaintTextFormat();
    Double_t orig_LabelSize = gStyle->GetLabelSize();

    gStyle->SetPalette(51);
    gStyle->SetMarkerSize(1.45);
    gStyle->SetMarkerColor(kWhite);
    gStyle->SetPaintTextFormat("4.2f");

    Int_t numPars = rFit.floatParsFinal().getSize();
    if (numPars < 5)
    { gStyle->SetMarkerSize(1.4); }
    else if (numPars < 10)
    { gStyle->SetMarkerSize(1.1); }
    else if (numPars < 20)
    { gStyle->SetMarkerSize(0.85); }
    else if (numPars < 40)
    { gStyle->SetMarkerSize(0.5); }
    else
    { gStyle->SetMarkerSize(0.25); }

    TH2D *h_corr = (TH2D *) rFit.correlationHist(Form("h_corr_%s",
                                                      rFit.GetName()));

    Double_t labelSize = orig_LabelSize;
    if (numPars < 5)
    { labelSize = 0.05; }
    else if (numPars < 10)
    { labelSize = 0.04; }
    else if (numPars < 20)
    { labelSize = 0.025; }
    else if (numPars < 40)
    { labelSize = 0.02; }
    else
    { labelSize = 0.015; }

    h_corr->GetXaxis()->SetLabelSize(labelSize);
    h_corr->GetYaxis()->SetLabelSize(labelSize);

    gPad->SetLeftMargin(0.18);
    gPad->SetRightMargin(0.13);

    //gStyle->SetMarkerSize(orig_MarkerSize);
    //gStyle->SetMarkerColor(orig_MarkerColor);
    //gStyle->SetPaintTextFormat(orig_PaintTextFormat);
    //gStyle->SetLabelSize(orig_LabelSize);
    gStyle->SetOptStat(00000000);

    h_corr->Draw("colz");
    h_corr->Draw("textsame");

    std::string path = outDir + "/" + canvName + ".pdf";
    c_corr.SaveAs(path.c_str());

    gStyle->SetMarkerSize(orig_MarkerSize);
    gStyle->SetMarkerColor(orig_MarkerColor);
    gStyle->SetPaintTextFormat(orig_PaintTextFormat);
    gStyle->SetLabelSize(orig_LabelSize);
    gStyle->SetOptStat(00000000);

    // cleanup
    delete h_corr;

    return path;
}
