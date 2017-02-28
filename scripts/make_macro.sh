#!/bin/sh
#
# Create an empty macro with given name
# 
# Usage: make_macro <macro_name>
#
# (C) 20150424 Max Baak
# Rewritten for macros 20170208 by Lodewijk Nauta

# -- Check number of arguments --
if [ $# -ne 2 ] ; then
   echo "usage: make_macro <output directory> <macro_name>"
   exit 1
fi

# -- Check existence of template files
template="${ESKAPADE}/templates/macro_template.py"
if [ ! -f  ] ; then
   echo "ERROR: Cannot find template file ${template}"
   exit 1
fi
if [ ! -d $1 ] ; then
   echo "ERROR: Cannot find output directory $1"
   exit 1
fi
if [ -z $ESKAPADE ] ; then
   echo "ERROR: You have not sourced the project. Type source setup.sh in the root of the project."
   exit 1
fi

directory=$1
macro=$2
tlmacro=`echo "$macro" | awk '{ print tolower($1) }'`

# -- Get current date and put it into a tmp file.
today="$(date +'%Y/%m/%d')"
sed "s,DATE,$today,g" "${template}" > "${template}.tmp"

# -- Make named instance of template
cat "${template}.tmp" | awk '{ gsub("MACROTEMPLATE","'$macro'") ; print $0}' > $directory/${tlmacro}.py

rm "${template}.tmp"

echo "Done, created:"
echo "$directory/${tlmacro}.py"

