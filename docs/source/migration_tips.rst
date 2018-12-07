From version 0.8 to 0.9
_______________________

In Eskapade v0.9 the core functionality has been migrated to the separate package Eskapade-Core.
We have tried to make this transition as seamless as possible, but you may well run into any migration issues.
In case you do below we list the changes needed to migrate from Eskapade version 0.8 to version 0.9.

* Whenever a line with ``import eskapade`` fails, simply replace ``eskapade`` with ``escore``.

* For example: ``from eskapade import core`` change to ``from escore import core``

That's it.


From version 0.6 to 0.7
_______________________

Below we list the API changes needed to migrate from Eskapade version 0.6 to version 0.7.

Links
:::::

* Process manager definition:

  - ``proc_mgr.`` change to ``process_manager.``
  - ``ProcessManager`` change to ``process_manager``
  - Delete line: ``proc_mgr = ProcessManager()``

* Logger:

  - Change ``log().`` to ``logger.``


Macros
::::::

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
:::::

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
