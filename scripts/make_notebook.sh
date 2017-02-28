#!/bin/sh
#
# Create an empty notebook with given name
# 
# Usage: make_notebook <notebook_name>
#
# (C) 20150424 Max Baak
# Rewritten for notebooks 20170208 by Lodewijk Nauta

# -- Check number of arguments --
if [ $# -ne 3 ] ; then
   echo "usage: make_notebook <notebook_directory> <notebook_name> <macro_name>"
   exit 1
fi

# -- Check existence of template files
template="${ESKAPADE}/templates/notebook_template.ipynb"
if [ ! -f  ] ; then
   echo "ERROR: Cannot find template file ${template}"
   exit 1
fi
if [ ! -d $1 ] ; then
   echo "ERROR: Cannot find output directory $1"
   exit 1
fi
if [ -z $ESKAPADE ] ; then
   echo "ERROR: Project is not sourced. Type source setup.sh in the root of the project."
   exit 1
fi
if [ -e $1/$2.ipynb ] ; then
   echo "Notebook already exists! Choose another name."
   exit 1
fi

directory=$1
notebook=$2
macro=$3

# -- Get current date and put it into a tmp file.
today="$(date +'%Y/%m/%d')"
sed "s,DATE,$today,g" "${template}" > "${template}.tmp"

# -- Make named instance of template
cat "${template}.tmp" | awk '{ gsub("MACRONAME","'$macro'") ; print $0}' > $directory/${notebook}.ipynb

rm "${template}.tmp"

echo "Done, created:"
echo "$directory/${notebook}.ipynb"
