#!/bin/bash

# setup Eskapade package
export ESKAPADE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# set path for Eskapade executables
export PATH="${ESKAPADE}/scripts":"${PATH}"

# set path for Eskapade Python modules
export PYTHONPATH="${ESKAPADE}/python":"${PYTHONPATH}"

# set path for Eskapade library
export LD_LIBRARY_PATH="${ESKAPADE}/lib":"${LD_LIBRARY_PATH}"

# Hack for ssh from mac 
export LC_ALL=C 
