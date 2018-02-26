
From version 0.6 to 0.7
-----------------------

Below we list the API changes needed to migrate from Eskapade version 0.6 to version 0.7.

Links
_____

* Process manager definition:

  - ``proc_mgr.`` change to ``process_manager.``
  - ``ProcessManager`` change to ``process_manager``
  - Delete line: ``proc_mgr = ProcessManager()``

* Logger:

  - Change ``log().`` to ``logger.``


Macros
______

* Process manager definition:

  - ``proc_mgr.`` change to ``process_manager.``
  - ``ProcessManager`` change to ``process_manager``
  - Delete line: ``proc_mgr = ProcessManager()``

* Logger:

  - ``import logging`` change to ``from eskapade.logger import Logger, LogLevel``
  - ``log.`` change to ``logger.``
  - ``log = logging.getLogger('macro.cpf_analysis')`` change to ``logger = Logger()``
  - ``logging`` change to ``LogLevel``
	
* Settings:
  
  Remove ``os.environ['WORKDIRROOT']``, since the environment variable WORKDIRROOT is no longer defined, define explicitly the data and macro paths,
  or execute the macros and tests from the root directory of the project, resulting in something like:

  - ``settings['resultsDir'] = os.getcwd() + 'es_results'``
  - ``settings['macrosDir']  = os.getcwd() + 'es_macros'``
  - ``settings['dataDir']    = os.getcwd() + 'data'``

* Chain definition in macros:

  - To import the Chain object add ``from eskapade import Chain``
  - Change ``process_manager.add_chain('chain_name')`` to ``<chain_name> = Chain('chain_name')``
  - ``process_manager.get_chain('ReadCSV').add_link``  to ``<chain_name>.add``


Tests
_____

* Process manager definition:

  - Change ``ProcessManager()``  to ``process_manager``
  - Change ``process_manager.get_chain`` to ``process_manager.get``

* Settings:
  
  Remove ``os.environ['WORKDIRROOT']``, since the environment variable WORKDIRROOT is no longer defined, define explicitly the data and macro paths,
  or execute the macros and tests from the root directory of the project, resulting in something like:

  - ``settings['macrosDir'] = os.getcwd() + '/es_macros'``
  - ``settings['dataDir']   = os.getcwd() + '/data'``

* StatusCode:

  - Change ``status.isSkipChain()`` to ``status.is_skip_chain()``
