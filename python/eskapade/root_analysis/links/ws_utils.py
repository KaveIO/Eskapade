# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : WsUtils                                                               *
# * Created: 2017/03/25                                                            *
# * Description:                                                                   *
# *      Algorithm to fill a RooWorkspace with useful objects and apply
# *      operations to them, such as simulation, fitting, and plotting.            *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import os
import uuid
import copy
import tabulate
import re

import ROOT

try:
    from ROOT import RooFit
except ImportError:
    import ROOT.RooFit

from eskapade import core
from eskapade import process_manager
from eskapade import ConfigObject
from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode

from eskapade.root_analysis import RooFitManager, roofit_utils
from eskapade.core import persistence


class WsUtils(Link):
    """Apply standard operations to object in the RooFit workspace

    Operations include:
    - moving object to and from the datastore/workspace
    - execute rooworkspace factory commands
    - simulation, use the function: add_simulate()
    - fitting, use the function: add_fit()
    - plotting, use the function add_plot()
    """

    def __init__(self, **kwargs):
        """Initialize WsUtils instance

        Note:
        - For simulations use the function: add_simulate()
        - For fitting use the function: add_fit()
        - For plotting use the function: add_plot()

        :param str name: name of link
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

    def initialize(self):
        """Initialize WsUtils"""

        # check input arguments
        self.check_arg_types(pages_key=str)

        if isinstance(self.copy_into_ws, str):
            self.copy_into_ws = [self.copy_into_ws]
        assert isinstance(self.copy_into_ws, list), 'copy_into_ws needs to be a string or list of strings.'

        if isinstance(self.copy_into_ds, str):
            self.copy_into_ds = [self.copy_into_ds]
        assert isinstance(self.copy_into_ds, list), 'copy_into_ds needs to be a string or list of strings.'

        # get I/O configuration
        io_conf = process_manager.service(ConfigObject).io_conf()

        # read report templates
        with open(core.persistence.io_path('templates', io_conf, 'df_summary_report.tex')) as templ_file:
            self.report_template = templ_file.read()
        with open(core.persistence.io_path('templates', io_conf, 'df_summary_report_page.tex')) as templ_file:
            self.page_template = templ_file.read()
        with open(persistence.io_path('templates', io_conf, 'df_summary_table_page.tex')) as templ_file:
            self.table_template = templ_file.read()

        # get path to results directory
        if not self.results_path:
            # get I/O configuration
            io_conf = process_manager.service(ConfigObject).io_conf()
            self.results_path = core.persistence.io_path('results_data', io_conf, 'report')

        # check if output directory exists
        if os.path.exists(self.results_path):
            # check if path is a directory
            if not os.path.isdir(self.results_path):
                self.log().critical('output path "%s" is not a directory', self.results_path)
                raise AssertionError('output path is not a directory')
        else:
            # create directory
            self.log().debug('Making output directory %s', self.results_path)
            os.makedirs(self.results_path)

        # make sure Eskapade RooFit library is loaded for fitting (for plotting correlation matrix)
        if self._fit:
            roofit_utils.load_libesroofit()

        return StatusCode.Success

    def execute(self):
        """Execute WsUtils

        Operations are executed in this order:

        1. put objects from the datastore into rooworkspace
        2. execute rooworkspace factory commands
        3. pass the workspace to (a list of) functions, to execute bits of (workspace) code
        4. simulate data from a pdf
        5. fit a pdf to a dataset
        6. make a plot of a dataset, pdf, or function
        7. move objects from the workspace to the datastore
        """

        proc_mgr = process_manager
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)
        ws = proc_mgr.service(RooFitManager).ws

        # --- open existing report pages
        if self.pages_key:
            self.pages = ds.get(self.pages_key, [])
            if not isinstance(self.pages, list):
                raise TypeError('pages key "{}" does not refer to a list'.format(self.pages_key))
            elif len(self.pages) > 0:
                self.log().debug('Retrieved %d report pages under key "%s"', len(self.pages), self.pages_key)

        # --- put objects from the datastore into the workspace
        #     by doing this here, the object can be picked up by the factory
        for key in self.copy_into_ws:
            assert key in ds, 'key "%s" not found in datastore' % key
            try:
                ws[key] = ds[key]
                if self.rm_original:
                    del ds[key]
            except BaseException:
                raise RuntimeError('could not import object "%s" into rooworkspace' % key)

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
            assert isinstance(tp, tuple) and len(tp) == 2, 'simulate item "%d" needs to be an args, kwargs tuple' % i
            self.do_simulate(ds, ws, *tp[0], **tp[1])

        # --- fitting
        #     needs input pdf and dataset to fit
        for i, tp in enumerate(self._fit):
            assert isinstance(tp, tuple) and len(tp) == 2, 'fit item "%d" needs to be an args, kwargs tuple' % i
            self.do_fit(ds, ws, *tp[0], **tp[1])

        # --- plotting
        #     needs single observable, pdf and/or datset
        for i, tp in enumerate(self._plot):
            assert isinstance(tp, tuple) and len(tp) == 2, 'plot item "%d" needs to be an args, kwargs tuple' % i
            self.do_plot(ds, ws, *tp[0], **tp[1])

        # --- storage into ws
        #     put objects from the workspace into the datastore
        for key in self.copy_into_ds:
            assert key in ws, 'key "%s" not found in workspace' % key
            try:
                ds[key] = ws[key].Clone()
                if self.rm_original:
                    self.rm_from_ws.append(key)
            except BaseException:
                raise RuntimeError('could not import object "%s" from workspace into ds' % key)

        # --- deletion
        #     try to remove keys from the workspace
        for key in self.rm_from_ws:
            try:
                ws.cd()
                ROOT.gDirectory.Delete("%s;*" % key)
            except BaseException:
                self.log().warning('Could not remove "%s" from workspace. Pass', key)

        # storage
        if self.pages_key:
            ds[self.pages_key] = self.pages
            self.log().debug('%d report pages stored under key: %s', len(self.pages), self.pages_key)

        return StatusCode.Success

    def finalize(self):
        """Finalize WsUtils"""

        # write report file
        if len(self.pages) == 0:
            return StatusCode.Success
        report_name = 'report_fit'
        if self.pages_key:
            report_name = re.sub('[^A-Za-z0-9_]+', '', self.pages_key)
        report_file_name = '{0}/{1}.tex'.format(self.results_path, report_name)
        with open(report_file_name, 'w') as report_file:
            report_file.write(self.report_template.replace('INPUT_PAGES', ''.join(self.pages)))
        self.log().debug('%s length: %d pages', report_name, len(self.pages))

        return StatusCode.Success

    def add_simulate(self, *args, **kwargs):
        """Add simulation task

        Stash args and kwargs, to be executed by do_simulate() during execute()

        :param args: positional arguments passed on to do_simulate()
        :param kwargs: key word arguments passed on to do_simulate()
        """

        a = copy.deepcopy(args)
        kw = copy.deepcopy(kwargs)
        self._simulate.append((a, kw))

    def do_simulate(self, ds, ws, pdf, num, obs, key='', into_ws=False, *args, **kwargs):
        """Simulate data based on input PDF

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
            thepdf = pdf
        else:
            assert len(pdf), 'pdf name not set'
            thepdf = ds[pdf] if pdf in ds else ws.pdf(pdf)
        if not thepdf:
            raise RuntimeError('unable to retrieve pdf')
        else:
            assert isinstance(thepdf, ROOT.RooAbsPdf)

        if isinstance(obs, ROOT.RooArgSet):
            theobs = obs
        elif isinstance(obs, str) and len(obs):
            theobs = ds[obs] if obs in ds else ws.set(obs)
        else:
            set_name = thepdf.GetName() + '_varset'
            theobs = ws.set(set_name)
        if not theobs:
            if isinstance(obs, str):
                # try to create a temporary observables set
                temp_obs = uuid.uuid4().hex
                failure = ws.defineSet(temp_obs, obs)
                if not failure:
                    theobs = ws.set(temp_obs)
                else:
                    raise RuntimeError('unable to retrieve (/create) observables with name "%s" for simulation' % obs)
            else:
                raise RuntimeError('unable to retrieve (/create) observables for simulation')
        else:
            assert isinstance(theobs, ROOT.RooArgSet)

        # process residual kwargs as roofit options
        roofit_opts = self._get_roofit_opts_list(ds, ws, **kwargs) if kwargs else ()
        roofit_opts += args

        # 3. generate data
        try:
            self.log().debug('Now generating %d records with pdf "%s" ...', num, thepdf.GetName())
            ROOT.RooAbsData.setDefaultStorageType(ROOT.RooAbsData.Tree)
            sim_data = thepdf.generate(theobs, num, *roofit_opts)
            if len(key) == 0:
                key = thepdf.GetName() + '_sim_data'
            sim_data.SetName(key)
            self.log().debug('Generated %d records with pdf "%s"', num, thepdf.GetName())
        except Exception as exc:
            # re-raise exeption if import failed
            self.log().error('Failed to generate data with pdf "%s"', thepdf.GetName())
            raise exc

        # 4. cleanup of temporary observables set
        if 'temp_obs' in vars():
            ws.removeSet(temp_obs)

        # 5. storage
        if into_ws:
            ws.put(sim_data)
        else:
            ds[key] = sim_data
        self.log().debug('Simulated data stored under key: %s', key)

    def add_fit(self, *args, **kwargs):
        """Add fit task

        Stash args and kwargs, to be executed by do_fit() during execute()

        :param args: positional arguments passed on to do_fit()
        :param kwargs: key word arguments passed on to do_fit()
        """
        a = copy.deepcopy(args)
        kw = copy.deepcopy(kwargs)
        self._fit.append((a, kw))

    def do_fit(self, ds, ws, pdf, data, key='', replace_existing=True, into_ws=False, *args, **kwargs):
        """Fit PDF to data set

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
            thepdf = pdf
        else:
            assert isinstance(pdf, str) and len(pdf), 'pdf name not set'
            thepdf = ds[pdf] if pdf in ds else ws.pdf(pdf)
        if not thepdf:
            raise RuntimeError('unable to retrieve pdf for fitting')
        else:
            assert isinstance(thepdf, ROOT.RooAbsPdf)

        if isinstance(data, ROOT.RooAbsData):
            thedata = data
        else:
            assert isinstance(data, str) and len(data), 'data set name not set'
            thedata = ds[data] if data in ds else ws.data(data)
        if not thedata:
            raise RuntimeError('unable to retrieve dataset for fitting')
        else:
            assert isinstance(thedata, ROOT.RooAbsData)

        # process residual kwargs as roofit options
        roofit_opts = self._get_roofit_opts_list(ds, ws, **kwargs) if kwargs else ()
        roofit_opts += args

        # fit pdf to data and store
        try:
            fit_result = thepdf.fitTo(thedata, RooFit.Save(), *roofit_opts)
            if len(key) == 0:
                key = thepdf.GetName() + '_fitTo_' + thedata.GetName()
            fit_result.SetName(key)
        except Exception as exc:
            # re-raise exeption if import failed
            self.log().error('Failed to fit data "%s" with pdf "%s"', thedata.GetName(), thepdf.GetName())
            raise exc

        # turn fit_result into latex report
        self._add_fit_result_to_report(fit_result)

        # storage
        if into_ws:
            ws.put(fit_result)
        else:
            ds[key] = fit_result
        self.log().debug('Fit result stored under key: %s', key)

    def _add_fit_result_to_report(self, fit_result):
        """Turn fit_result into latex report

        :param ROOT.RooFitResult fit_result: fit result
        """

        if not isinstance(fit_result, ROOT.RooFitResult):
            raise AssertionError('input fit result object not of type RooFitResult')

        fit_result_name = re.sub('[^A-Za-z0-9_]+', '', fit_result.GetName())

        # process correlation matrix from fit result
        corr_label = 'correlation matrix of ' + fit_result_name
        n_pars = fit_result.floatParsFinal().getSize()
        cov_qual = fit_result.covQual()
        fit_status = fit_result.status()
        min_nll = fit_result.minNll()
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
            fp_table = '\input{%s}' % fp_file_name
            fp_init = fit_result.floatParsInit()
            fp_final.printLatex(RooFit.Sibling(fp_init), RooFit.OutputFile(fp_file_name))
            self.pages.append(self.table_template.replace('VAR_LABEL', fp_label).replace('VAR_STATS_TABLE', fp_table))

        # table of constant fit parameters
        const_pars = fit_result.constPars()
        if const_pars.getSize() > 0:
            cp_label = 'constant fit parameters of ' + fit_result_name
            cp_file_name = self.results_path + '/const_pars_' + fit_result_name + '.tex'
            cp_table = '\input{%s}' % cp_file_name
            const_pars.printLatex(RooFit.OutputFile(cp_file_name))
            self.pages.append(self.table_template.replace('VAR_LABEL', cp_label).replace('VAR_STATS_TABLE', cp_table))

    def add_plot(self, *args, **kwargs):
        """Add plotting task

        Stash args and kwargs, to be executed by do_fit() during execute()

        :param args: positional arguments passed on to do_plot()
        :param kwargs: key word arguments passed on to do_plot()
        """

        a = copy.deepcopy(args)
        kw = copy.deepcopy(kwargs)
        self._plot.append((a, kw))

    def do_plot(self, ds, ws, obs, data=None, pdf=None, func=None, data_args=(), pdf_args=(), func_args=(),
                data_kwargs={}, pdf_kwargs={}, func_kwargs={}, key='', into_ws=False, output_file=None, bins=40,
                logy=False, miny=0, plot_range=None):
        """Make a plot of data and/or a pdf, or of a function

        Either a dataset, pdf, or function needs to be provided as input for plotting.

        :param ds: input data store, from which to retrieve pdf and dataset to fit
        :param ROOT.RooWorkspace ws: input workspace, from which to retrieve pdf and dataset to fit
        :param data: input dataset used for plotting, can be a key to look up or RooAbsData (optional)
        :param pdf: input pdf used for plotting, can be a key to look up or RooAbsPdf (optional)
        :param func: input function used for plotting, can be a key to look up or RooAbsReal (optional)
        :param data_args: positional arguments passed on to the plotting of the data. (optional)
        :param data_kwargs: key word arguments passed on to the plotting of the data. (optional)
        :param pdf_args: positional arguments passed on to the plotting of the pdf. (optional)
        :param pdf_kwargs: key word arguments passed on to the plotting of the pdf. (optional)
        :param func_args: positional arguments passed on to the plotting of the function. (optional)
        :param func_kwargs: key word arguments passed on to the plotting of the function. (optional)
        :param str key: key under which to store the plot frame (=RooPlot).
                        If key exists in ds/workspace, plot in the existing frame. (optional)
        :param bool into_ws: if true, store simulated data in workspace, not the datastore
        :param str output_file: if set, store plot with this file name (optional)
        :param int bins: number of bins in the plot. default is 40. (optional)
        :param bool logy: if true, set y-axis to log scale (optional)
        :param float miny: set minimum value of y-axis to miny value (optional)
        :param tuple plot_range: specify x-axis plot range as (min, max) (optional)
        """

        # basic checks
        assert pdf is not None or data is not None or func is not None, 'both pdf, dataset, and func not set'

        # retrieve obs, data, and pdf
        if isinstance(obs, ROOT.RooRealVar):
            theobs = obs
        else:
            assert isinstance(obs, str) and len(obs), 'obs name for plotting not set'
            theobs = ds[obs] if obs in ds else ws.var(obs)
        if not theobs:
            raise RuntimeError('unable to retrieve observable for plotting')
        else:
            assert isinstance(theobs, ROOT.RooRealVar)

        if isinstance(data, ROOT.RooAbsData):
            thedata = data
        elif isinstance(data, str) and len(data):
            thedata = ds[data] if data in ds else ws.data(data)
            if not thedata:
                raise RuntimeError('unable to retrieve dataset with name "%s" from workspace' % data)
            else:
                assert isinstance(thedata, ROOT.RooAbsData)

        if isinstance(pdf, ROOT.RooAbsPdf):
            thepdf = pdf
        elif isinstance(pdf, str) and len(pdf):
            thepdf = ds[pdf] if pdf in ds else ws.pdf(pdf)
            if not thepdf:
                raise RuntimeError('unable to retrieve pdf with name "%s" from workspace' % pdf)
            else:
                assert isinstance(thepdf, ROOT.RooAbsPdf)

        if isinstance(func, ROOT.RooAbsReal):
            thefunc = func
        elif isinstance(func, str) and len(func):
            thefunc = ds[func] if func in ds else ws.function(func)
            if not thefunc:
                raise RuntimeError('unable to retrieve function with name "%s" from workspace' % func)
            else:
                assert isinstance(thefunc, ROOT.RooAbsReal)

        # process pdf, data, and func args and kwargs
        pdf_opts = self._get_roofit_opts_list(ds, ws, **pdf_kwargs) if pdf_kwargs else ()
        if len(pdf_args) == 1:
            pdf_args = (pdf_args[0],)
        pdf_opts += pdf_args
        data_opts = self._get_roofit_opts_list(ds, ws, **data_kwargs) if data_kwargs else ()
        if len(data_args) == 1:
            pdf_args = (data_args[0],)
        data_opts += data_args
        func_opts = self._get_roofit_opts_list(ds, ws, **func_kwargs) if func_kwargs else ()
        if len(func_args) == 1:
            pdf_args = (func_args[0],)
        func_opts += func_args

        # plot on existing RooPlot? If so, retrieve.
        if not plot_range:
            plot_range = (theobs.getMin(), theobs.getMax())
        assert len(plot_range) == 2, 'plot range needs to be a tuple of two floats'
        if key:
            if isinstance(key, ROOT.RooPlot):
                frame = key
            else:
                assert isinstance(key, str) and len(key), 'key for rooplot needs to be a filled string'
                frame = ds[key] if key in ds else ws.obj(key)
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
        """Add plot to pdflatex report

        :param str file_path: path of pdf input plot
        """

        label = os.path.splitext(file_path.split('/')[-1])[0]
        self.pages.append(self.page_template.replace('VAR_LABEL', label).replace('VAR_STATS_TABLE', '')
                          .replace('VAR_HISTOGRAM_PATH', file_path))

    def _get_roofit_opts_list(self, ds, ws, *args, **kwargs):
        """Return RooFit cmd options for fitting and plotting

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
            assert isinstance(o, str) and len(o), 'RooFit function name not specified'
            if type(v) not in [list, tuple]:
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
        """Retrieve object from either datastore or workspace

        Search order is first datastore, then workspace.

        :param str key: key to search form
        :param ds: reference to datastore
        :param ws: reference to rooworkspace
        """

        # key needs to be a filled key I can pick up.
        assert isinstance(key, str) and len(key), 'key needs to be a filled string'
        assert ds or ws, 'provide either datastore or workspace to search in'

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
