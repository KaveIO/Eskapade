# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Class  : ReadFromRootFile                                                    *
# * Created: 2017/06/26                                                          *
# * Description:                                                                 *
# *      Simple link to read objects from a ROOT file an put them into           *
# *      the data store or workspace                                             *
# *                                                                              *
# * Authors:                                                                     *
# *      KPMG Big Data team, Amstelveen, The Netherlands                         *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import os

import ROOT

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
from eskapade.root_analysis import RooFitManager


class ReadFromRootFile(Link):
    """Put objects from a ROOT file in the data store or workspace"""

    def __init__(self, **kwargs):
        """Initialize ReadFromRootFile instance

        :param str name: name of link
        :param str path: path of your input root file
        :param list keys: keys to pick up from root file
        :param bool into_ws: if true, store in workspace instead of data store (default is False)
        """

        # initialize link and process arguments
        Link.__init__(self, kwargs.pop('name', 'ReadFromRootFile'))
        self._process_kwargs(kwargs,
                             path='',
                             keys=[],
                             into_ws=False)
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self.in_file = None

    def initialize(self):
        """Initialize ReadFromRootFile"""

        # check arguments
        self.check_arg_types(path=str)
        self.check_arg_types(recurse=True, keys=str)

        # check if input file exists
        if not os.path.exists(self.path):
            self.log().error('Input file "%s" not found', self.path)
            raise AssertionError('input file not found')

        self.in_file = ROOT.TFile(self.path)
        if self.in_file.IsZombie():
            self.log().error('Input file "%s" not a valid ROOT file', self.path)
            raise AssertionError('input file not a valid ROOT file')

        return StatusCode.Success

    def execute(self):
        """Execute ReadFromRootFile"""

        proc_mgr = ProcessManager()
        ds = proc_mgr.service(DataStore)
        if self.into_ws:
            ws = proc_mgr.service(RooFitManager).ws

        for key in self.keys:
            obj = self.in_file.Get(key)
            if not obj:
                self.log().warning('Object with key "%s" not found in "%s"; skipping', key, self.path)
                continue
            # a. put object into the workspace
            if self.into_ws:
                try:
                    ws[key] = obj
                except BaseException:
                    raise RuntimeError('could not import object "{}" into workspace'.format(key))
            # b. put object into datastore
            else:
                ds[key] = obj

        return StatusCode.Success

    def finalize(self):
        """Finalize ReadFromRootFile"""

        if self.in_file:
            self.in_file.Close()

        return StatusCode.Success
