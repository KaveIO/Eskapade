#! /bin/bash
set -e

# variables
ESUSER="esdev"
VHOSTNAMES="es-host es-mongo es-jboss es-rabbitmq es-service"
ESMOUNTID="esrepo"
TMPDIR="/tmp/esinstall"
LOGDIR="/var/log/esinstall"
ESDIR="/opt/eskapade"
KTBRELEASE="3.0-Beta"
KTBDIR="/opt/KaveToolbox"
ANADIR="/opt/anaconda"
SPARKRELEASE="2.1.0"
SPARKDIR="/opt/spark"
ROOTRELEASE="6.08.06"
ROOTDIR="/opt/root"
PYCHARMRELEASE="2016.3.2"
PYCHARMDIR="/opt/pycharm"

# log function
function log {
  echo "$(date +'%F %T %Z'): $@"
}

# set non-interactive front end
export DEBIAN_FRONTEND="noninteractive"

# let "sh" be "bash" instead of "dash"
ln -sf /bin/bash /bin/sh

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
apt-get -y install openjdk-8-jdk gfortran cmake libgsl-dev libfftw3-dev dpkg-dev libxpm-dev libxft-dev libxext-dev\
  freeglut3-dev libxml2-dev graphviz-dev\
  locales locales-all texlive &> "${LOGDIR}/install.log"

# install KAVE Toolbox
log "installing KAVE Toolbox"
cd "${TMPDIR}"
wget -q "http://repos:kaverepos@repos.dna.kpmglab.com/noarch/KaveToolbox/${KTBRELEASE}/kavetoolbox-installer-${KTBRELEASE}.sh"
mkdir -p /etc/kave
cp /vagrant/ktb/CustomInstall.py /vagrant/ktb/requirements.txt /etc/kave/
bash "kavetoolbox-installer-${KTBRELEASE}.sh" --node &> "${LOGDIR}/install-ktb.log"

# source KAVE setup in both login and non-login shells (interactive)
mv /etc/profile.d/kave.sh "${KTBDIR}/pro/scripts/"
sed -i -e "s|/etc/profile\.d/kave\.sh|${KTBDIR}/pro/scripts/kave.sh|g" /etc/bash.bashrc

# setup Spark environment
sed -e "s|SPARK_HOME_VAR|${SPARKDIR}/pro|g" /vagrant/spark/spark_env.sh >> "${KTBDIR}/pro/scripts/KaveEnv.sh"
mkdir -p "${SPARKDIR}"
ln -sfT "spark-${SPARKRELEASE}" "${SPARKDIR}/pro"

# install Spark
log "installing Spark in ${SPARKDIR}/spark-${SPARKRELEASE}"
cd "${TMPDIR}"
wget -q "http://archive.apache.org/dist/spark/spark-${SPARKRELEASE}/spark-${SPARKRELEASE}.tgz"
tar -xzf "spark-${SPARKRELEASE}.tgz" --no-same-owner -C "${SPARKDIR}"
cd "${SPARKDIR}/pro"
build/mvn -DskipTests clean package &> "${LOGDIR}/install-spark.log"

# setup ROOT environment
sed -e "s|ROOTSYS_VAR|${ROOTDIR}/pro|g" /vagrant/root/root_env.sh >> "${KTBDIR}/pro/scripts/KaveEnv.sh"
mkdir -p "${ROOTDIR}"
ln -sfT "root-${ROOTRELEASE}" "${ROOTDIR}/pro"

# fetch ROOT
cd "${TMPDIR}"
wget -q "https://root.cern.ch/download/root_v${ROOTRELEASE}.source.tar.gz"
tar -xzf "root_v${ROOTRELEASE}.source.tar.gz" --no-same-owner

# apply ROOT patches
log "applying ROOT patches"
cd "root-${ROOTRELEASE}"
for patchfile in $(ls /vagrant/root/patches/*.patch); do
  patch -p1 -i "${patchfile}"
done

# install ROOT
log "installing ROOT in ${ROOTDIR}/root-${ROOTRELEASE}"
cd "${TMPDIR}"
mkdir -p root_build
cd root_build
cmake -DCMAKE_INSTALL_PREFIX="${ROOTDIR}/root-${ROOTRELEASE}" \
  -Dfail-on-missing=ON \
  -Dcxx14=ON -Droot7=ON -Dshared=ON -Dsoversion=ON -Dthread=ON -Dfortran=ON -Dpython=ON -Dcling=ON -Dx11=ON -Dssl=ON \
  -Dxml=ON -Dfftw3=ON -Dbuiltin_fftw3=OFF -Dmathmore=ON -Dminuit2=ON -Droofit=ON -Dtmva=ON -Dopengl=ON -Dgviz=ON \
  -Dalien=OFF -Dbonjour=OFF -Dcastor=OFF -Dchirp=OFF -Ddavix=OFF -Ddcache=OFF -Dfitsio=OFF -Dgfal=OFF -Dhdfs=OFF \
  -Dkrb5=OFF -Dldap=OFF -Dmonalisa=OFF -Dmysql=OFF -Dodbc=OFF -Doracle=OFF -Dpgsql=OFF -Dpythia6=OFF -Dpythia8=OFF \
  -Dsqlite=OFF -Drfio=OFF -Dxrootd=OFF \
  -DPYTHON_EXECUTABLE="${ANADIR}/pro/bin/python" \
  -DNUMPY_INCLUDE_DIR="${ANADIR}/pro/lib/python3.5/site-packages/numpy/core/include" \
  "../root-${ROOTRELEASE}" &> "${LOGDIR}/install-root.log"
cmake --build . --target install -- -j4 &>> "${LOGDIR}/install-root.log"

# install Python packages for ROOT
log "installing Python packages for ROOT"
cd "${TMPDIR}"
git clone git://github.com/rootpy/root_numpy.git
bash -c "source ${KTBDIR}/pro/scripts/KaveEnv.sh && python ${TMPDIR}/root_numpy/setup.py install"\
    &> "${LOGDIR}/install-root-python.log"

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
