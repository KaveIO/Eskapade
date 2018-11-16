# --- base image: latest known kavetoolbox
FROM kave/kavetoolbox:3.6-Beta.u16.node

# --- me
MAINTAINER Max Baak <baak.max@kpmg.nl>

# --- fix http://stackoverflow.com/questions/22466255/is-it-possibe-to-answer-dialog-questions-when-installing-under-docker
ARG DEBIAN_FRONTEND=noninteractive

# --- link sh to bash
SHELL ["/bin/bash", "-c"]

# --- Eskapade location
ENV ESKAPADE "/opt/eskapade"

# --- KAVE setup script
ENV KAVE_ENV_SCRIPT "/opt/KaveToolbox/pro/scripts/KaveEnv.sh"

# --- Eskapade directories
RUN mkdir ${ESKAPADE} \
    && mkdir /opt/esroofit \
    && mkdir /opt/esspark

# --- useful Ubuntu packages, e.g. pdflatex. 
RUN apt-get update
RUN apt-get install -y --no-install-recommends texlive-latex-base texlive-latex-extra texlive-fonts-recommended pandoc aspell

# --- trigger automatic installation of required Spark dependencies. Requires Spark setup.
RUN echo "print('Hello world!')" > /tmp/test.py \
    && source "${KAVE_ENV_SCRIPT}" \
    && /opt/spark/pro/bin/spark-submit --packages org.diana-hep:histogrammar-sparksql_2.11:1.0.4 /tmp/test.py \
    && rm /tmp/test.py

# --- append sourcing of Kave env to bashrc
COPY bashrc_appendum /tmp/
RUN cat /tmp/bashrc_appendum >> "${HOME}"/.bashrc && rm /tmp/bashrc_appendum

