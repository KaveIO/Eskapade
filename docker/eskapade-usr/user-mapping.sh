#!/bin/bash

# username in container
if [ -z "${USER}" ]; then
  echo "USER environment variable not set."; exit 100
fi

# if both not set, print warning and set to default values
if [ -z "${HOST_USER_ID}" -a -z "${HOST_USER_GID}" ]; then
    echo ""
    echo "To apply uid/gid mapping HOST_USER_ID and HOST_USER_GID environment variables must be set. E.g.:"
    echo "$ docker run -e HOST_USER_ID=$(id -u) -e HOST_USER_GID=$(id -g) -it kave/eskapade-usr:latest"
    echo ""
fi

# if not set, set uid/gid to default values 1000
if [ -z "${HOST_USER_ID}" ]; then
    HOST_USER_ID=1000
fi
if [ -z "${HOST_USER_GID}" ]; then
    HOST_USER_GID=1000
fi

# current user and group id
CURRENT_UID=`id -u ${USER}`
CURRENT_GID=`id -g ${USER}`

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
if [[ "${HOST_USER_ID}" -ne 1000 ]] || [[ "${HOST_USER_GID}" -ne 1000 ]]; then
    if [[ "${HOST_USER_ID}" -ne "${CURRENT_UID}" ]] || [[ "${HOST_USER_GID}" -ne "${CURRENT_GID}" ]]; then
        echo 'Applying user mapping to set permissions for' ${USER_HOME} '(this may take some time)'
        chown -R ${USER_ID}:${USER_GID} ${USER_HOME}
    fi
fi

echo 'Switching to user' ${USER}
su - "${USER}"
