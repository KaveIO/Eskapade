Eskapade subpackages
====================

core
----

Core of the Eskapade framework of chains with analysis links


core_ops
--------

Links for basic chain and data operations

* dependencies: core
* tutorials: esk1*


analysis
--------

Basic analysis tools

* dependencies: core, core_ops
* tutorials: esk2*


visualization
-------------

Tools for visualization of analysis results

* dependencies: core, core_ops, analysis
* tutorials: esk3*


root_analysis
-------------

Interfaces for analysis with the ROOT framework

* dependencies: core, core_ops, analysis, visualization
* tutorials: esk4*


data_quality
------------

Tools to assess and improve data quality

* dependencies: core, core_ops, analysis
* tutorials: esk5*


spark_analysis
--------------

Spark related analysis links

* dependencies: core, core_ops, analysis
* tutorials: esk6*