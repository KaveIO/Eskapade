#!/bin/bash

# setup Eskapade package
export ESKAPADE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# put python & root stuff into PATH, LD_LIBRARY_PATH
export PATH=.:${ESKAPADE}/scripts:${PATH}

# PYTHONPATH
export PYTHONPATH=${ESKAPADE}/python:$PYTHONPATH

# Hack for ssh from mac 
export LC_ALL=C 
