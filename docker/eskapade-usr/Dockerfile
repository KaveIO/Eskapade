# --- base image: latest known Eskapade environment
FROM kave/eskapade-env:0.7

# --- me
MAINTAINER Max Baak <baak.max@kpmg.nl>

# --- link sh to bash
SHELL ["/bin/bash", "-c"]

# --- user mapping script from https://gist.github.com/renzok/29c9e5744f1dffa392cf
COPY user-mapping.sh / 
RUN  chmod u+x /user-mapping.sh 

# --- create dedicated Eskapade user
ENV USER=esdev USER_ID=1000 USER_GID=1000
RUN groupadd --gid "${USER_GID}" "${USER}" && \
    useradd \
      --uid ${USER_ID} \
      --gid ${USER_GID} \
      --create-home \
      --shell /bin/bash \
      ${USER}

# --- switch to Eskapade user
USER esdev
WORKDIR /home/esdev

# --- append sourcing of Kave env to bashrc
COPY bashrc_appendum /tmp/
RUN cat /tmp/bashrc_appendum >> "${HOME}"/.bashrc 

# --- setup user's own Eskapade environment
RUN . /opt/KaveToolbox/pro/scripts/KaveEnv.sh && \
    conda create -y -n eskapade_env python=3.6 && \
    . activate eskapade_env && \
    cd /home/esdev && cp -rv /opt/eskapade . && \
    rm -rf eskapade/build eskapade/python/eskapade/lib && \
    rm -r $(find . -iname __pycache__) && \
    pip install -e eskapade

# --- apply user mapping
USER root
ENTRYPOINT ["/user-mapping.sh"] 
