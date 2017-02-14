import kavedefaults as cnf

# install Python 3.5
cnf.conda.python = 3

# do not install ROOT from Anaconda (manual install later)
cnf.root.doInstall = False
cnf.gsl.doInstall = False

# upgrade ROOT from version 6.04 to 6.06 (6.04 only for Python 3.4)
cnf.root.version = '6.06'

# remove installation of "root_pandas" from install script (only available for Python 3.4)
def _root_script(self):
    self.run("bash -c 'source " + self.toolbox.envscript() + " > /dev/null ; conda config --add channels NLESC;'")
    self.run("bash -c 'source " + self.toolbox.envscript()
             + " > /dev/null ; conda config --add channels https://conda.anaconda.org/nlesc/label/dev;'")
    self.run("bash -c 'source " + self.toolbox.envscript()
             + " > /dev/null ; conda install root=" + self.version + " --yes;'")
    self.run("bash -c 'source " + self.toolbox.envscript()
             + " > /dev/null ; conda install rootpy root-numpy --yes;'")
type(cnf.root).script = _root_script
