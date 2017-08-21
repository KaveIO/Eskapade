import os
import subprocess
import platform

from setuptools import Extension
from setuptools.command.build_ext import build_ext


class CMakeExtension(Extension):
    def __init__(self, name: str, source_dir: str = '.'):
        Extension.__init__(self, name, sources=[])
        self.source_dir = os.path.abspath(os.path.expanduser(source_dir))


class CMakeBuild(build_ext):
    def run(self):
        out = ''
        try:
            out = subprocess.check_output(['cmake', '--version'],
                                          stderr=subprocess.STDOUT)
        except OSError:
            raise RuntimeError(out + '\n' +
                               'CMake is required to install the following extensions: ' +
                               ', '.join(e.name for e in self.extensions))

        if platform.system() == "Windows":
            raise RuntimeError("Windows is not supported!")

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        build_type = 'Debug' if self.debug else 'Release'

        ext_dir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))

        cmake_args = ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + ext_dir]
        cmake_args += ['-DCMAKE_BUILD_TYPE=' + build_type]

        build_args = ['--config', build_type]
        build_args += ['--', '-j2']

        env = os.environ.copy()
        env['CXXFLAGS'] = '{cxx_flags} -DVERSION_INFO="{version}"'.format(cxx_flags=env.get('CXXFLAGS', ''),
                                                                          version=self.distribution.get_version())

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        subprocess.check_call(['cmake', ext.source_dir] + cmake_args, cwd=self.build_temp, env=env)
        subprocess.check_call(['cmake', '--build', '.'] + build_args, cwd=self.build_temp)

