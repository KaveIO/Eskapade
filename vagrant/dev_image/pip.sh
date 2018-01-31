#! /bin/bash

# log function
function log {
  echo "$(date +'%F %T %Z'): $@"
}

# (re-)install eskapade
log "(re-)installing ESKAPADE"
. /opt/KaveToolbox/pro/scripts/KaveEnv.sh
pip install -e /opt/eskapade
