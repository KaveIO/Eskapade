# --- base image: latest known Eskapade environment
FROM kave/eskapade-base:0.8.3

# --- me
MAINTAINER Max Baak <baak.max@kpmg.nl>

# --- useful Python package(s); for ROOT packages need Kave env first.
# --- Eskapade-ROOT: pip does not pick up custom installation from pypi repo, 
#     but does so from git when installing in editable mode.
#     See: https://stackoverflow.com/questions/19569557/pip-not-picking-up-a-custom-install-cmdclass

ENV KAVE_ENV_SCRIPT "/opt/KaveToolbox/pro/scripts/KaveEnv.sh"

RUN source "${KAVE_ENV_SCRIPT}" \
    && /opt/anaconda/pro/bin/pip install sphinx_rtd_theme==0.4.1 \
    && /opt/anaconda/pro/bin/pip install Eskapade==0.8.2 \
    && /opt/anaconda/pro/bin/pip install -e git+https://github.com/KaveIO/Eskapade-ROOT.git@v0.8.3#egg=eskapade-root \
    && /opt/anaconda/pro/bin/pip install Eskapade-Spark==0.8.2
