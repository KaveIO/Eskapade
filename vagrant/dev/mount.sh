#! /bin/bash

ESMOUNTID="esrepo"

# log function
function log {
  echo "$(date +'%F %T %Z'): $@"
}

# unmount all repository mounts
log "unmounting all ESKAPADE repository mounts"
for i in $(seq $(findmnt -n ${ESMOUNTID} | wc -l)); do
  umount ${ESMOUNTID}
done

# (re-)mount repository
log "(re-)mounting ESKAPADE repository"
mount ${ESMOUNTID}
