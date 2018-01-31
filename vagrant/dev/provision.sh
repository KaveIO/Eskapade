#! /bin/bash
set -e

# variables
ESUSER="esdev"
VHOSTNAMES="es-host es-mongo es-proxy es-rabbitmq es-service"
ESMOUNTID="esrepo"
TMPDIR="/tmp/esinstall"
LOGDIR="/var/log/esinstall"
ESDIR="/opt/eskapade"
KTBRELEASE="3.6-Beta"
KTBDIR="/opt/KaveToolbox"
ANADIR="/opt/anaconda"
PYCHARMRELEASE="2017.2.3"
PYCHARMDIR="/opt/pycharm"

# log function
function log {
  echo "$(date +'%F %T %Z'): $@"
}

# set non-interactive front end
export DEBIAN_FRONTEND="noninteractive"

# create directories for software installation
mkdir -p "${TMPDIR}"
mkdir -p "${LOGDIR}"
cd "${TMPDIR}"

# set names of host machine
hostip=$(ip route | awk '/default/ { print $3 }')
log "associating host names \"${VHOSTNAMES}\" to IP address ${hostip}"
echo -e "${hostip} ${VHOSTNAMES}" >> /etc/hosts

# create ESKAPADE user
log "creating ${ESUSER} user (password \"${ESUSER}\")"
adduser --disabled-login --gecos "" "${ESUSER}"
echo "${ESUSER}:${ESUSER}" | chpasswd

# enable login as ESKAPADE user with key
log "authorizing key \"esdev_id_rsa\" for ${ESUSER}"
mkdir -p "/home/${ESUSER}/.ssh"
cat /vagrant/ssh/esdev_id_rsa.pub >> "/home/${ESUSER}/.ssh/authorized_keys"
chown -R "${ESUSER}":"${ESUSER}" "/home/${ESUSER}/.ssh/"
chmod -R go-rwx "/home/${ESUSER}/.ssh"

# set up mounting of ESKAPADE repository
mkdir -p "${ESDIR}"
echo "${ESMOUNTID} ${ESDIR} vboxsf rw,nodev,uid=$(id -u ${ESUSER}),gid=$(id -g ${ESUSER}) 0 0" >> /etc/fstab
echo "vboxsf" >> /etc/modules
sudo -u "${ESUSER}" ln -s "${ESDIR}" "/home/${ESUSER}/$(basename ${ESDIR})"

# update system
log "updating package manager"
apt-get -y update &> "${LOGDIR}/update.log"
log "upgrading system"
apt-get -y dist-upgrade &> "${LOGDIR}/dist-upgrade.log"
log "installing additional packages"
apt-get -y install python &> "${LOGDIR}/install.log"

# set locale properly
log "setting locale"
echo -e "LC_ALL=en_US.UTF-8" >> /etc/default/locale

# install KAVE Toolbox with customised installation script
log "installing KAVE Toolbox"
cd "${TMPDIR}"
wget -q "http://repos:kaverepos@repos.dna.kpmglab.com/noarch/KaveToolbox/${KTBRELEASE}/kavetoolbox-installer-${KTBRELEASE}.sh"
mkdir -p /etc/kave
cp /vagrant/ktb/CustomInstall.py /etc/kave/
bash "kavetoolbox-installer-${KTBRELEASE}.sh" --node &> "${LOGDIR}/install-ktb.log"

# install additional packages
log "installing additional packages for Eskapade"
apt-get install -y --no-install-recommends mongodb-clients &> "${LOGDIR}/install-additional.log"

# install Eskapade and its Python requirements
log "installing Eskapade"
source ${KTBDIR}/pro/scripts/KaveEnv.sh
"${ANADIR}/pro/bin/pip" install -e "${ESDIR}" &> "${LOGDIR}/install-Python-requirements.log"
log "installing Python requirements"
"${ANADIR}/pro/bin/conda" install -y django pymongo sphinx_rtd_theme &>> "${LOGDIR}/install-Python-requirements.log"
"${ANADIR}/pro/bin/pip" install djangorestframework markdown django-filter celery cherrypy names jaydebeapi \
	&>> "${LOGDIR}/install-Python-requirements.log"

# source KAVE setup in both login and non-login shells (interactive)
mv /etc/profile.d/kave.sh "${KTBDIR}/pro/scripts/"
sed -i -e "s|/etc/profile\.d/kave\.sh|${KTBDIR}/pro/scripts/kave.sh|g" /etc/bash.bashrc

# setup PyCharm environment
sed -e "s|PYCHARM_HOME_VAR|${PYCHARMDIR}/pro|g" /vagrant/pycharm/pycharm_env.sh >> "${KTBDIR}/pro/scripts/KaveEnv.sh"
mkdir -p "${PYCHARMDIR}"
ln -sfT "pycharm-community-${PYCHARMRELEASE}" "${PYCHARMDIR}/pro"

# install PyCharm
log "installing PyCharm in ${PYCHARMDIR}/pycharm-community-${PYCHARMRELEASE}"
cd "${TMPDIR}"
wget -q "https://download.jetbrains.com/python/pycharm-community-${PYCHARMRELEASE}.tar.gz"
tar -xzf "pycharm-community-${PYCHARMRELEASE}.tar.gz" --no-same-owner -C "${PYCHARMDIR}"

# install Lubuntu desktop
log "installing desktop environment"
apt-get -y install lubuntu-desktop &>> "${LOGDIR}/install-desktop.log"

# general configuration for ESKAPADE user
cp /vagrant/bash/bashrc "/home/${ESUSER}/.bashrc"
cat /vagrant/bash/bash_aliases >> "/home/${ESUSER}/.bash_aliases"
cp /vagrant/vim/vimrc "/home/${ESUSER}/.vimrc"

# clean up
cd /
rm -rf "${TMPDIR}"
