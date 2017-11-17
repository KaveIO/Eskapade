"""Project: Eskapade - A python-based package for data analysis.

Class : WsUtils

Created: 2017/03/25

Description:
    Algorithm to fill a RooWorkspace with useful objects and apply
    operations to them, such as simulation, fitting, and plotting.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import copy
import os
import re
import uuid

import tabulate
import pandas as pd
import ROOT
from ROOT import RooFit

from eskapade import process_manager, resources, DataStore, Link, StatusCode
from eskapade.core import persistence
from eskapade.root_analysis import roofit_utils
from eskapade.root_analysis.roofit_manager import RooFitManager


class WsUtils(Link):
    """Apply standard operations to object in the RooFit workspace.

    Operations include:
    - moving object to and from the datastore/workspace
    - execute rooworkspace factory commands
    - simulation, use the function: add_simulate()
    - fitting, use the function: add_fit()
    - plotting, use the function add_plot()
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        Note:
        - For simulations use the function: add_simulate()
        - For fitting use the function: add_fit()
        - For plotting use the function: add_plot()

        :param str name: name of link
        :param bool clear_workspace: if true, clear workspace at start of execute.
        :param list copy_into_ws: key of input data to read from data store, to be inserted in rooworkspace
        :param list copy_into_ds: key of input data to read from rooworkspace, to be inserted in data store
        :param bool rm_original: if true, objects inserted in ws/ds are removed from ds/ws. Default is false.
        :param list rm_from_ws: rm keys from rooworkspace
        :param list factory: list of commands passed to workspace factory at execute()
        :param list apply: list of functions to pass workspace through at execute()
        :param str results_path: output path of plot (optional)
        :param str pages_key: data store key of existing report pages
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'WsUtils'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             clear_workspace=False,
                             copy_into_ws=[],
                             copy_into_ds=[],
                             rm_original=False,
                             rm_from_ws=[],
                             factory=[],
                             apply=[],
                             results_path='',
                             pages_key='')

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self._simulate = []
        self._fit = []
        self._plot = []
        self.pages = []

    def _process_results_path(self):
        """Process results_path argument."""
        if not self.results_path:
            self.results_path = persistence.io_path('results_data', 'report')
        persistence.create_dir(self.results_path)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(pages_key=str)

        if isinstance(self.copy_into_ws, str):
            self.copy_into_ws = [self.copy_into_ws]
        assert isinstance(self.copy_into_ws, list), 'copy_into_ws needs to be a string or list of strings.'

        if isinstance(self.copy_into_ds, str):
            self.copy_into_ds = [self.copy_into_ds]
        assert isinstance(self.copy_into_ds, list), 'copy_into_ds needs to be a string or list of strings.'

        # read report templates
        with open(resources.template('df_summary_report.tex')) as templ_file:
            self.report_template = templ_file.read()
        with open(resources.template('df_summary_report_page.tex')) as templ_file:
            self.page_template = templ_file.read()
        with open(resources.template('df_summary_table_page.tex')) as templ_file:
            self.table_template = templ_file.read()

        self._process_results_path()

        # make sure Eskapade RooFit library is loaded for fitting (for plotting correlation matrix)
        if self._fit:
            roofit_utils.load_libesroofit()

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Operations are executed in this order:

        1. put objects from the datastore into rooworkspace
        2. execute rooworkspace factory commands
        3. pass the workspace to (a list of) functions, to execute bits of (workspace) code
        4. simulate data from a pdf
        5. fit a pdf to a dataset
        6. make a plot of a dataset, pdf, or function
        7. move objects from the workspace to the datastore
        """
        ds = process_manager.service(DataStore)
        rfm = process_manager.service(RooFitManager)

        # --- open existing report pages
        if self.pages_key:
            self.pages = ds.get(self.pages_key, [])
            if not isinstance(self.pages, list):
                raise TypeError('Pages key "{}" does not refer to a list'.format(self.pages_key))
            elif len(self.pages) > 0:
                self.logger.debug('Retrieved {n:d} report pages under key "{key}".', n=len(self.pages),
                                  key=self.pages_key)

        # --- delete existing workspace?
        if self.clear_workspace:
            rfm.delete_workspace()
        ws = rfm.ws

        # --- put objects from the datastore into the workspace
        #     by doing this here, the object can be picked up by the factory
        for key in self.copy_into_ws:
            assert key in ds, 'Key "{}" not found in datastore.'.format(key)
            try:
                ws[key] = ds[key]
                if self.rm_original:
                    del ds[key]
            except BaseException:
                raise RuntimeError('Could not import object "{}" into rooworkspace.'.format(key))

        # --- workspace factory commands
        #     by doing this here, the object previously imported objects
        #     can be picked up by the factory
        for cmd in self.factory:
            ws.factory(cmd)

        # --- pass ws to list of functions, to execute bits of (workspace) code
        #     by doing this here, the objects previously created can be picked up.
        for func in self.apply:
            func(ws)

        # --- simulation
        #     needs input pdf and observables to generate
        for i, tp in enumerate(self._simulate):
            assert isinstance(tp, tuple) and len(tp) == 2, \
                'simulate item "{:d}" needs to be an args, kwargs tuple.'.format(i)
            self.do_simulate(ds, ws, *tp[0], **tp[1])

        # --- fitting
        #     needs input pdf and dataset to fit
        for i, tp in enumerate(self._fit):
            assert isinstance(tp, tuple) and len(tp) == 2, \
                'fit item "{:d}" needs to be an args, kwargs tuple.'.format(i)
            self.do_fit(ds, ws, *tp[0], **tp[1])

        # --- plotting
        #     needs single observable, pdf and/or datset
        for i, tp in enumerate(self._plot):
            assert isinstance(tp, tuple) and len(tp) == 2, \
                'plot item "{:d}" needs to be an args, kwargs tuple.'.format(i)
            self.do_plot(ds, ws, *tp[0], **tp[1])

        # --- storage into ws
        #     put objects from the workspace into the datastore
        for key in self.copy_into_ds:
            assert key in ws, 'Key "{}" not found in workspace.'.format(key)
            try:
                ds[key] = ws[key].Clone()
                if self.rm_original:
                    self.rm_from_ws.append(key)
            except BaseException:
                raise RuntimeError('Could not import object "{}" from workspace into ds.'.format(key))

        # --- deletion
        #     try to remove keys from the workspace
        for key in self.rm_from_ws:
            try:
                ws.cd()
                ROOT.gDirectory.Delete("{};*".format(key))
            except BaseException:
                self.logger.warning('Could not remove "{key}" from workspace. Pass.', key=key)

        # storage
        if self.pages_key:
            ds[self.pages_key] = self.pages
            self.logger.debug('{n:d} report pages stored under key: {key}.', n=len(self.pages), key=self.pages_key)

        return StatusCode.Success

    def finalize(self):
        """Finalize the link."""
        # write report file
        if not self.pages:
            return StatusCode.Success

        report_name = 'report_fit'
        if self.pages_key:
            report_name = re.sub('[^A-Za-z0-9_]+', '', self.pages_key)
        report_file_name = '{0}/{1}.tex'.format(self.results_path, report_name)
        with open(report_file_name, 'w') as report_file:
            report_file.write(self.report_template.replace('INPUT_PAGES', ''.join(self.pages)))
        self.logger.debug('{length} length: {n_pages:d} pages', length=report_name, n_pages=len(self.pages))

        return StatusCode.Success

    def add_simulate(self, *args, **kwargs):
        """Add simulation task.

        Stash args and kwargs, to be executed by do_simulate() during execute()

        :param args: positional arguments passed on to do_simulate()
        :param kwargs: key word arguments passed on to do_simulate()
        """
        a = copy.deepcopy(args)
        kw = copy.deepcopy(kwargs)
        self._simulate.append((a, kw))

    def do_simulate(self, ds, ws, pdf, num, obs, key='', into_ws=False, *args, **kwargs):
        """Simulate data based on input PDF.

        :param ds: input data store, from which to retrieve pdf and observables
        :param ROOT.RooWorkspace ws: input workspace, from which to retrieve pdf and observables
        :param pdf: input pdf used for simulation, can be a key to look up or RooAbsPdf
        :param int num: number of records to generate
        :param obs: input observables used for simulation, can be a key to look up or a RooArgSet
        :param str key: key under which to store the simulated data
        :param bool into_ws: if true, store simulated data in workspace, not the datastore
        :param args: all other positional arguments are passed on to the roofit generate function.
        :param kwargs: all other key word arguments are passed on to the roofit generate function.
        """
        # 1. basic checks
        assert num > 0, 'number of entries needs to be a positive integer'

        # 2. retrieve pdf and observables
        if isinstance(pdf, ROOT.RooAbsPdf):
            the_pdf = pdf
        else:
            assert len(pdf), 'pdf name not set'
            the_pdf = ds.get(pdf, ws.pdf(pdf))
        if not the_pdf:
            raise RuntimeError('unable to retrieve pdf')
        else:
            assert isinstance(the_pdf, ROOT.RooAbsPdf)

        if isinstance(obs, ROOT.RooArgSet):
            the_obs = obs
        elif isinstance(obs, str) and obs:
            the_obs = ds.get(obs, ws.set(obs))
        else:
            set_name = the_pdf.GetName() + '_varset'
            the_obs = ws.set(set_name)
        if not the_obs:
            if isinstance(obs, str):
                # try to create a temporary observables set
                temp_obs = uuid.uuid4().hex
                failure = ws.defineSet(temp_obs, obs)
                if not failure:
                    the_obs = ws.set(temp_obs)
                else:
                    raise RuntimeError('Unable to retrieve (/create) observables with name "{}" for simulation.'
                                       .format(obs))
            else:
                raise RuntimeError('Unable to retrieve (/create) observables for simulation.')
        else:
            assert isinstance(the_obs, ROOT.RooArgSet)

        # process residual kwargs as roofit options
        roofit_opts = self._get_roofit_opts_list(ds, ws, **kwargs) if kwargs else ()
        roofit_opts += args

        # 3. generate data
        try:
            self.logger.debug('Now generating {n:d} records with pdf "{name}" ...', n=num, name=the_pdf.GetName())
            ROOT.RooAbsData.setDefaultStorageType(ROOT.RooAbsData.Tree)
            sim_data = the_pdf.generate(the_obs, num, *roofit_opts)
            if not key:
                key = the_pdf.GetName() + '_sim_data'
            sim_data.SetName(key)
            self.logger.debug('Generated {n:d} records with pdf "{name}".', n=num, name=the_pdf.GetName())
        except Exception as exc:
            # re-raise exeption if import failed
            self.logger.error('Failed to generate data with pdf "{name}".', name=the_pdf.GetName())
            raise exc

        # 4. cleanup of temporary observables set
        if 'temp_obs' in vars():
            ws.removeSet(temp_obs)

        # 5. storage
        if into_ws:
            ws.put(sim_data)
        else:
            ds[key] = sim_data
        self.logger.debug('Simulated data stored under key: {key}.', key=key)

    def add_fit(self, *args, **kwargs):
        """Add fit task.

        Stash args and kwargs, to be executed by do_fit() during execute()

        :param args: positional arguments passed on to do_fit()
        :param kwargs: key word arguments passed on to do_fit()
        """
        a = copy.deepcopy(args)
        kw = copy.deepcopy(kwargs)
        self._fit.append((a, kw))

    def do_fit(self, ds, ws, pdf, data, key='', into_ws=False, *args, **kwargs):
        """Fit PDF to data set.

        :param ds: input data store, from which to retrieve pdf and dataset to fit
        :param ROOT.RooWorkspace ws: input workspace, from which to retrieve pdf and dataset to fit
        :param pdf: input pdf used for fitting, can be a key to look up or RooAbsPdf
        :param data: input dataset used for fitting, can be a key to look up or RooAbsData
        :param str key: key under which to store the fit result object
        :param bool into_ws: if true, store simulated data in workspace, not the datastore
        :param args: all other positional arguments are passed on to the roofit fit function.
        :param kwargs: all other key word arguments are passed on to the roofit fit function.
        """
        # basic checks
        if isinstance(pdf, ROOT.RooAbsPdf):
            the_pdf = pdf
        else:
            assert isinstance(pdf, str) and pdf, 'pdf name not set'
            the_pdf = ds.get(pdf, ws.pdf(pdf))
        if not the_pdf:
            raise RuntimeError('unable to retrieve pdf for fitting')
        else:
            assert isinstance(the_pdf, ROOT.RooAbsPdf)

        if isinstance(data, ROOT.RooAbsData):
            the_data = data
        else:
            assert isinstance(data, str) and data, 'data set name not set'
            the_data = ds.get(data, ws.data(data))
        if not the_data:
            raise RuntimeError('unable to retrieve dataset for fitting')
        else:
            assert isinstance(the_data, ROOT.RooAbsData)

        # process residual kwargs as roofit options
        roofit_opts = self._get_roofit_opts_list(ds, ws, **kwargs) if kwargs else ()
        roofit_opts += args

        # fit pdf to data and store
        try:
            fit_result = the_pdf.fitTo(the_data, RooFit.Save(), *roofit_opts)
            if not key:
                key = the_pdf.GetName() + '_fitTo_' + the_data.GetName()
            fit_result.SetName(key)
        except Exception as exc:
            # re-raise exeption if import failed
            self.logger.error('Failed to fit data "{data_name}" with pdf "{pdf_name}"', data_name=the_data.GetName(),
                              pdf_name=the_pdf.GetName())
            raise exc

        # turn fit_result into latex and dataframe report
        fr_df = self._make_fit_result_report(fit_result)

        # storage
        if into_ws:
            ws[key] = fit_result
        else:
            ds[key] = fit_result
        self.logger.debug('Fit result stored under key: {key}.', key=key)

        ds[key + '_df'] = fr_df
        self.logger.debug('Fit result dataframe stored at: {key}', key=key + '_df')

    def _make_fit_result_report(self, fit_result):
        """Turn fit_result into latex and DataFrame report.

        :param ROOT.RooFitResult fit_result: fit result
        :returns: fit result turned in to pandas dataframe
        :rtype: pandas.DataFrame
        """
        if not isinstance(fit_result, ROOT.RooFitResult):
            raise AssertionError('Input fit result object not of type RooFitResult.')

        fit_result_name = re.sub('[^A-Za-z0-9_]+', '', fit_result.GetName())

        # results to be kept in pandas dataframe
        cols = ['covqual', 'status', 'minnll']
        row = []

        # process correlation matrix from fit result
        corr_label = 'correlation matrix of ' + fit_result_name
        n_pars = fit_result.floatParsFinal().getSize()
        cov_qual = fit_result.covQual()
        fit_status = fit_result.status()
        min_nll = fit_result.minNll()
        row += [cov_qual, fit_status, min_nll]

        table = []
        table.append(('number pars', '{:d}'.format(n_pars)))
        table.append(('cov.qual (ok=3)', '{:d}'.format(cov_qual)))
        table.append(('status (ok=0)', '{:d}'.format(fit_status)))
        table.append(('nll', '{:f}'.format(min_nll)))
        corr_table = tabulate.tabulate(table, tablefmt='latex')
        corr_file_name = ROOT.Eskapade.PlotCorrelationMatrix(fit_result, self.results_path)
        self.pages.append(self.page_template.replace('VAR_LABEL', corr_label).replace('VAR_STATS_TABLE', corr_table)
                          .replace('VAR_HISTOGRAM_PATH', corr_file_name))

        # table of floating fit parameters
        fp_final = fit_result.floatParsFinal()
        if fp_final.getSize() > 0:
            fp_label = 'floating fit parameters of ' + fit_result_name
            fp_file_name = self.results_path + '/floating_pars_' + fit_result_name + '.tex'
            fp_table = '\input{{{}}}'.format(fp_file_name)
            fp_init = fit_result.floatParsInit()
            fp_final.printLatex(RooFit.Sibling(fp_init), RooFit.OutputFile(fp_file_name))
            self.pages.append(self.table_template.replace('VAR_LABEL', fp_label).replace('VAR_STATS_TABLE', fp_table))

        # table of constant fit parameters
        const_pars = fit_result.constPars()
        if const_pars.getSize() > 0:
            cp_label = 'constant fit parameters of ' + fit_result_name
            cp_file_name = self.results_path + '/const_pars_' + fit_result_name + '.tex'
            cp_table = '\input{{{}}}'.format(cp_file_name)
            const_pars.printLatex(RooFit.OutputFile(cp_file_name))
            self.pages.append(self.table_template.replace('VAR_LABEL', cp_label).replace('VAR_STATS_TABLE', cp_table))

        # pandas dataframe of fit result
        if fp_final.getSize() > 0:
            for fp in fp_final:
                par_name = re.sub('[^A-Za-z0-9_]+', '', fp.GetName())
                cols += [par_name, '%s_err' % par_name]
                row += [fp.getVal(), fp.getError()]
        fit_result_df = pd.DataFrame([row], columns=cols)

        return fit_result_df

    def add_plot(self, *args, **kwargs):
        """Add plotting task.

        Stash args and kwargs, to be executed by do_fit() during execute()

        :param args: positional arguments passed on to do_plot()
        :param kwargs: key word arguments passed on to do_plot()
        """
        a = copy.deepcopy(args)
        kw = copy.deepcopy(kwargs)
        self._plot.append((a, kw))

    def do_plot(self, ds, ws, obs, data=None, pdf=None, func=None, data_args=(), pdf_args=(), func_args=(),
                data_kwargs=None, pdf_kwargs=None, func_kwargs=None, key='', into_ws=False, output_file=None, bins=40,
                logy=False, miny=0, maxy=None, plot_range=None):
        """Make a plot of data and/or a pdf, or of a function.

        Either a dataset, pdf, or function needs to be provided as input for plotting.

        :param ds: input data store, from which to retrieve pdf and dataset to fit
        :param ROOT.RooWorkspace ws: input workspace, from which to retrieve pdf and dataset to fit
        :param data: input dataset used for plotting, can be a key to look up or RooAbsData. Optional.
        :param pdf: input pdf used for plotting, can be a key to look up or RooAbsPdf. Optional.
        :param func: input function used for plotting, can be a key to look up or RooAbsReal. Optional.
        :param data_args: positional arguments passed on to the plotting of the data. Optional.
        :param data_kwargs: key word arguments passed on to the plotting of the data. Optional.
        :param pdf_args: positional arguments passed on to the plotting of the pdf. Optional.
        :param pdf_kwargs: key word arguments passed on to the plotting of the pdf. Optional.
        :param func_args: positional arguments passed on to the plotting of the function. Optional.
        :param func_kwargs: key word arguments passed on to the plotting of the function. Optional.
        :param str key: key under which to store the plot frame (=RooPlot).
                        If key exists in ds/workspace, plot in the existing frame. Optional.
        :param bool into_ws: if true, store simulated data in workspace, not the datastore
        :param str output_file: if set, store plot with this file name. Optional.
        :param int bins: number of bins in the plot. default is 40. Optional.
        :param bool logy: if true, set y-axis to log scale. Optional.
        :param float miny: set minimum value of y-axis to miny value. Optional.
        :param float maxy: set maximum value of y-axis to maxy value. Optional.
        :param tuple plot_range: specify x-axis plot range as (min, max). Optional.
        """
        # basic checks
        assert pdf is not None or data is not None or func is not None, 'both pdf, dataset, and func not set'

        # retrieve obs, data, and pdf
        if isinstance(obs, ROOT.RooRealVar):
            theobs = obs
        else:
            assert isinstance(obs, str) and obs, 'obs name for plotting not set.'
            theobs = ds.get(obs, ws.var(obs))
        if not theobs:
            raise RuntimeError('Unable to retrieve observable for plotting.')
        else:
            assert isinstance(theobs, ROOT.RooRealVar)

        if isinstance(data, ROOT.RooAbsData):
            thedata = data
        elif isinstance(data, str) and data:
            thedata = ds.get(data, ws.data(data))
            if not thedata:
                raise RuntimeError('Unable to retrieve dataset with name "{}" from workspace.'.format(data))
            else:
                assert isinstance(thedata, ROOT.RooAbsData)

        if isinstance(pdf, ROOT.RooAbsPdf):
            thepdf = pdf
        elif isinstance(pdf, str) and pdf:
            thepdf = ds.get(pdf, ws.pdf(pdf))
            if not thepdf:
                raise RuntimeError('Unable to retrieve pdf with name "{}" from workspace.'.format(pdf))
            else:
                assert isinstance(thepdf, ROOT.RooAbsPdf)

        if isinstance(func, ROOT.RooAbsReal):
            thefunc = func
        elif isinstance(func, str) and func:
            thefunc = ds.get(func, ws.function(func))
            if not thefunc:
                raise RuntimeError('Unable to retrieve function with name "{}" from workspace.'.format(func))
            else:
                assert isinstance(thefunc, ROOT.RooAbsReal)

        # process pdf, data, and func args and kwargs
        pdf_opts = self._get_roofit_opts_list(ds, ws, **pdf_kwargs) if pdf_kwargs else ()
        if len(pdf_args) == 1:
            pdf_args = (pdf_args[0],)
        pdf_opts += pdf_args
        data_opts = self._get_roofit_opts_list(ds, ws, **data_kwargs) if data_kwargs else ()
        if len(data_args) == 1:
            data_args = (data_args[0],)
        data_opts += data_args
        func_opts = self._get_roofit_opts_list(ds, ws, **func_kwargs) if func_kwargs else ()
        if len(func_args) == 1:
            func_args = (func_args[0],)
        func_opts += func_args

        # plot on existing RooPlot? If so, retrieve.
        if not plot_range:
            plot_range = (theobs.getMin(), theobs.getMax())
        assert len(plot_range) == 2, 'plot range needs to be a tuple of two floats'
        if key:
            if isinstance(key, ROOT.RooPlot):
                frame = key
            else:
                assert isinstance(key, str) and key, 'Key for rooplot needs to be a filled string.'
                frame = ds.get(key, ws.obj(key))
                if not frame:
                    frame = theobs.frame(ROOT.RooFit.Bins(bins), ROOT.RooFit.Range(plot_range[0], plot_range[1]))
        else:
            frame = theobs.frame(ROOT.RooFit.Bins(bins), ROOT.RooFit.Range(plot_range[0], plot_range[1]))
        assert isinstance(frame, ROOT.RooPlot)

        # do the plotting on this frame
        c = ROOT.TCanvas()
        c.SetCanvasSize(1200, 800)
        if logy:
            c.SetLogy()
        c.cd()

        # order of plotting is first data then pdf.
        # this ensures that pdf is normalized to the dataset
        if 'thedata' in vars():
            thedata.plotOn(frame, *data_opts)
        if 'thepdf' in vars():
            thepdf.plotOn(frame, *pdf_opts)
        if 'thefunc' in vars():
            thefunc.plotOn(frame, *func_opts)

        # plot frame
        if output_file:
            if miny != 0:
                frame.SetMinimum(miny)
            if maxy is not None:
                assert isinstance(maxy, float) or isinstance(maxy, int), 'maxy needs to be a float.'
                frame.SetMaximum(maxy)
            frame.Draw()

        # store rooplot
        if key:
            if into_ws:
                ws.put(frame)
            else:
                ds[key] = frame
        # store picture as file
        if output_file:
            # store file in correct output directory
            output_file = self.results_path + '/' + output_file.split('/')[-1]
            c.SaveAs(output_file)
            # add plot to latex report
            self._add_plot_to_report(output_file)
        del c

    def _add_plot_to_report(self, file_path):
        """Add plot to pdflatex report.

        :param str file_path: path of pdf input plot
        """
        label = os.path.splitext(file_path.split('/')[-1])[0]
        self.pages.append(self.page_template.replace('VAR_LABEL', label).replace('VAR_STATS_TABLE', '')
                          .replace('VAR_HISTOGRAM_PATH', file_path))

    def _get_roofit_opts_list(self, ds, ws, *args, **kwargs):
        """Return RooFit cmd options for fitting and plotting.

        All key-word arguments are parsed as RooFit functions with certain settings.
        A setting can be a tuple or list of arguments.
        Example: LineColor=2, VisualizeError=['fit_result']

        The function below parses the arguments given to the roofit functions.
        If an argument is a string, interpret as a key and try to pick up arg from datastore or workspace.

        :param ds: reference to datastore to retrieve string arguments
        :param ws: reference to rooworkspace to retrieve string arguments
        :param kwargs: all key word arguments are interpreted as roofit function
        :param args: all other positional arguments are appended to interpreted kwargs
        :returns: argument tuple consisting or args and parsed kwargs
        """
        # loop through kwargs and parse arguments given to roofit functions.
        # if arg is a string, interpret as a key and try to pick up arg from ds or ws

        # FIXME: possible conflict when wishing to give a string as an option to a RooFit function,
        #        and this string also exists as key in the datastore or workspace
        opts = {}
        for o, v in kwargs.items():
            assert isinstance(o, str) and o, 'RooFit function name not specified.'
            if not isinstance(v, (list, tuple)):
                v = [v]
            margs = []
            for arg in v:
                if not isinstance(arg, str):
                    margs.append(arg)
                    continue
                # if arg is a string, try to pick up key from the datastore or workspace.
                obj = self._retrieve(arg, ds, ws)
                margs.append(obj if obj else arg)
            opts[o] = tuple(margs)

        # create roofit cmd arguments, and append to roofit argument tuple
        opts_list = []
        for o, v in opts.items():
            try:
                cmd = getattr(RooFit, o)(*v)
                # python does not take ownership of cmd
                ROOT.SetOwnership(cmd, False)
            except BaseException:
                continue
            opts_list.append(cmd)
        opts_list = (opts_list[0],) if len(opts_list) == 1 else tuple(opts_list)

        # append any other arguments given to function
        opts_list += args

        return opts_list

    def _retrieve(self, key, ds=None, ws=None):
        """Retrieve object from either datastore or workspace.

        Search order is first datastore, then workspace.

        :param str key: key to search form
        :param ds: reference to datastore
        :param ws: reference to rooworkspace
        """
        # key needs to be a filled key I can pick up.
        assert isinstance(key, str) and len(key), 'Key needs to be a filled string.'
        assert ds or ws, 'Provide either datastore or workspace to search in.'

        # first try datastore
        if ds:
            if key in ds:
                return ds[key]

        # below go through various get functions of rooworkspace ...
        if ws:
            # rooargset
            obj = ws.set(key)
            if obj:
                return obj
            # rooabsreal function
            obj = ws.function(key)
            if obj:
                return obj
            # roodataset or roodatahist
            obj = ws.data(key)
            if obj:
                return obj
            # roocategory
            obj = ws.cat(key)
            if obj:
                return obj
            # roorealvar
            obj = ws.var(key)
            if obj:
                return obj
            # generic root TObject
            obj = ws.obj(key)
            if obj:
                return obj

        # nothing found
        return None
