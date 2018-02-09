#!/bin/bash

# username in container
if [ -z "${USER}" ]; then
  echo "USER environment variable not set."; exit 100
fi

# if both not set we do not need to do anything
if [ -z "${HOST_USER_ID}" -a -z "${HOST_USER_GID}" ]; then
    echo "Both HOST_USERD_ID and HOST_USER_GID environment variables should be set." ; exit 0
fi

# reset user_id to either new id or if empty old (still one of above might not be set)
USER_ID=${HOST_USER_ID:=$USER_ID}
USER_GID=${HOST_USER_GID:=$USER_GID}

LINE=$(grep -F "${USER}" /etc/passwd)
# replace all ':' with a space and create array
array=( ${LINE//:/ } )

# home is 5th element
USER_HOME=${array[4]}

sed -i -e "s/^${USER}:\([^:]*\):[0-9]*:[0-9]*/${USER}:\1:${USER_ID}:${USER_GID}/"  /etc/passwd
sed -i -e "s/^${USER}:\([^:]*\):[0-9]*/${USER}:\1:${USER_GID}/"  /etc/group

# chown directory (if docker host/container id's do not match)
if [[ "${HOST_USER_ID}" -ne 1000 ]] && [[ "${HOST_USER_GID}" -ne 1000 ]]; then
    echo 'Applying user mapping to set permissions for' ${USER_HOME} '(this may take some time)'
    chown -R ${USER_ID}:${USER_GID} ${USER_HOME}
fi

echo 'Switching to user' ${USER}
su - "${USER}"
