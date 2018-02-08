# --- base image: latest known kavetoolbox
FROM kave/kavetoolbox:3.6-Beta.u16.node

# --- me
MAINTAINER Max Baak <baak.max@kpmg.nl>

# --- fix http://stackoverflow.com/questions/22466255/is-it-possibe-to-answer-dialog-questions-when-installing-under-docker
ARG DEBIAN_FRONTEND=noninteractive

# --- link sh to bash
SHELL ["/bin/bash", "-c"]

# --- Python package(s) for ROOT, needs Kave env.
ENV KAVE_ENV_SCRIPT "/opt/KaveToolbox/pro/scripts/KaveEnv.sh"
RUN source "${KAVE_ENV_SCRIPT}" \
    && /opt/anaconda/pro/bin/pip install root_numpy==4.7.3 \
    && /opt/anaconda/pro/bin/pip install rootpy==1.0.1

# --- Eskapade location
ENV ESKAPADE "/opt/eskapade"

# --- installation of Eskapade through tar file
COPY eskapade.tar.gz ./
RUN source "${KAVE_ENV_SCRIPT}" \
    && mkdir /opt/eskapade \
    && tar -xzf eskapade.tar.gz -C /opt/eskapade \
    && /opt/anaconda/pro/bin/pip install -e /opt/eskapade

# --- useful Python packages
RUN /opt/anaconda/pro/bin/pip install sphinx_rtd_theme

# --- useful Ubuntu packages, e.g. pdflatex. 
RUN apt-get update
RUN apt-get install -y --no-install-recommends texlive-latex-base texlive-latex-extra texlive-fonts-recommended pandoc

# --- useful Conda packages
RUN  /opt/anaconda/pro/bin/conda install -y django pymongo

# --- trigger automatic installation of required Spark dependencies. Requires Spark setup.
RUN echo "print('Hello world!')" > /tmp/test.py \
    && source "${KAVE_ENV_SCRIPT}" \
    && /opt/spark/pro/bin/spark-submit --packages org.diana-hep:histogrammar-sparksql_2.11:1.0.4 /tmp/test.py \
    && rm /tmp/test.py

# --- append sourcing of Kave env to bashrc
COPY bashrc_appendum /tmp/
RUN cat /tmp/bashrc_appendum >> "${HOME}"/.bashrc && rm /tmp/bashrc_appendum
