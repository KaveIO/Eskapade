#!/bin/sh
#
# Create an empty algorithm with given name
# 
# Usage: make_link <packagedir> <linkmname>
#
# (C) 20150424 Max Baak
#

# -- Check number of arguments --
if [ $# -ne 2 ] ; then
   echo "usage: make_link <output directory> <linkname>"
   exit 1
fi

# -- Check existence of template files
template="`dirname \"$0\"`/../templates/link_template.py"
if [ ! -f  ] ; then
   echo "ERROR: Cannot find template file ${template}"
   exit 1
fi
if [ ! -d $1 ] ; then
   echo "ERROR: Cannot find output directory $1"
   exit 1
fi

directory=$1
link=$2
# -- to lower case
tllink=`echo "$link" | awk '{ print tolower($1) }'`

# -- Get current date and put it into a tmp file.
today="$(date +'%Y/%m/%d')"
sed "s,DATE,$today,g" "${template}" > "${template}.tmp"

# -- Make named instance of template
cat "${template}.tmp" | awk '{ gsub("LINKTEMPLATE","'$link'") ; print $0}' > $directory/${tllink}.py

rm "${template}.tmp"

echo "> Done, created the skeleton:"
echo "$directory/${tllink}.py"
echo ""
echo "> Now edit this file:"
echo "$directory/__init__.py"
echo ""
echo "> Add \"$link\" to __all__ and add the line:" 
echo "from .${tllink} import $link"

