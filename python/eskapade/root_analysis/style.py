"""Project: Eskapade - A Python-based package for data analysis.

Module: root_analysis.style

Created: 2017/04/24

Description:
    Default ROOT style parameters for Eskapade

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import ROOT

# ROOT plot style
ROOT.gStyle.SetPaperSize(20, 26)
ROOT.gStyle.SetPadLeftMargin(0.15)
ROOT.gStyle.SetPadRightMargin(0.05)
ROOT.gStyle.SetPadBottomMargin(0.15)
ROOT.gStyle.SetPadTopMargin(0.05)
ROOT.gStyle.SetOptTitle(0)
ROOT.gStyle.SetOptStat(0)
ROOT.gStyle.SetTextSize(0.05)
ROOT.gStyle.SetLabelSize(0.05, 'x')
ROOT.gStyle.SetLabelSize(0.05, 'y')
ROOT.gStyle.SetLabelSize(0.05, 'z')
ROOT.gStyle.SetTitleSize(0.06, 'x')
ROOT.gStyle.SetTitleSize(0.06, 'y')
ROOT.gStyle.SetTitleSize(0.06, 'z')
ROOT.gStyle.SetLabelOffset(0.01, 'x')
ROOT.gStyle.SetLabelOffset(0.01, 'y')
ROOT.gStyle.SetTitleOffset(1.1, 'x')
ROOT.gStyle.SetTitleOffset(1.1, 'y')
