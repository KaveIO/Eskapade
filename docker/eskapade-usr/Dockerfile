# --- base image: latest known Eskapade environment
FROM kave/eskapade-base:0.8.4

# --- me
MAINTAINER Max Baak <baak.max@kpmg.nl>

# --- link sh to bash
SHELL ["/bin/bash", "-c"]

# --- user mapping script from https://gist.github.com/renzok/29c9e5744f1dffa392cf
COPY user-mapping.sh / 
RUN  chmod u+x /user-mapping.sh 

# --- fix https://kpmgnl.atlassian.net/browse/KTB-131
COPY kave.sh /etc/profile.d/kave.sh

# --- create dedicated Eskapade user
ENV USER=esdev USER_ID=1000 USER_GID=1000
RUN groupadd --gid "${USER_GID}" "${USER}" && \
    useradd \
      --uid ${USER_ID} \
      --gid ${USER_GID} \
      --create-home \
      --shell /bin/bash \
      ${USER}

# --- conda fix for making python envs visible in jupyter
#     see: https://stackoverflow.com/questions/39604271/conda-environments-not-showing-up-in-jupyter-notebook
#     for miniconda use the conda-forge channel
RUN . /opt/KaveToolbox/pro/scripts/KaveEnv.sh \
    && conda install nb_conda_kernels -y

# --- switch to Eskapade user
USER esdev
WORKDIR /home/esdev

# --- append sourcing of Kave env to bashrc
COPY bashrc_appendum /tmp/
RUN cat /tmp/bashrc_appendum >> "${HOME}"/.bashrc 

# --- setup user's own python Eskapade environment
COPY eskapade_env.yml /tmp/
RUN . /opt/KaveToolbox/pro/scripts/KaveEnv.sh \
    && conda env create -f /tmp/eskapade_env.yml -q --force

# --- install Eskapade packages
RUN . /opt/KaveToolbox/pro/scripts/KaveEnv.sh \
    && . activate eskapade_env \
    && pip install Eskapade==0.8.2 \
    && pip install -e git+https://github.com/KaveIO/Eskapade-ROOT.git@v0.8.3#egg=eskapade-root \
    && pip install Eskapade-Spark==0.8.2 \
    && pip install rootpy==1.0.1 \
    && pip install sphinx_rtd_theme==0.4.1 \
    && pip install pymongo==3.4.0 \
    && pip install jaydebeapi==1.1.1 \
    && pip install django==2.0.5 \
    && pip install nbconvert==5.1.1 \
    && pip install nbformat==4.3.0

# --- Eskapade-ROOT: pip does not pick up custom installation from pypi repo, 
#     but does so from git when installing in editable mode.
#     See: https://stackoverflow.com/questions/19569557/pip-not-picking-up-a-custom-install-cmdclass

# --- apply user mapping
USER root
ENTRYPOINT ["/user-mapping.sh"] 
