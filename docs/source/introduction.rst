============
Introduction
============

Welcome to Eskapade! This is a short introduction of the package and why we built it. In the next sections
we will go over the installation, a tutorial on how to run Eskapade properly, some examples use-cases,
and how to develop analysis code and run it in Jupyter and PyCharm.

What is Eskapade?
-----------------

Eskapade is an abbreviation for: 'Enterprise Solution KPMG Advanced Predictive Analytics Decision Engine'.
It is a framework to make your data analytics modular. This results in faster roll-out of analytics
solutions in your business and less overhead when taking multiple analyses in production. In particular, it is
intended for building Machine Learning models that are retrained when a certain trigger is reached.

Why did we build this?
----------------------

We found that the implementation phase of a data analytics solution at clients - a careful process - is often
completely different from building the solution itself - which proceeds through explorative iterations.
Therefore we made this analysis framework that makes it easier to set up a
data analysis, while simultaneously making it easier to put it into production. 

Next to that, it makes analyses modular, which has a lot of advantages. It is easier to work with multiple
people on the same project, because the contributions are divided in clear blocks. Re-use of code becomes more
straightforward, as old code is already put in a block with a clear purpose. Lastly, it gives you a universal
basis for all your analyses, which can both be used across a company, or for different clients. 

More about the purpose can be read at the general `readme <http://github.com/kaveio/eskapade>`_.

Naming convention
-----------------

Before settling on the name `Eskapade`, this project had internal naming conventions
including `Decision Engine` and `Analytics Engine`. We are working on removing all old names and
streamlining this all into future versions, but one might still find these in certain parts of the repository.
