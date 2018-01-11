from setuptools import setup

NAME = 'eskapade_python'


def setup_package() -> None:
    """
    The main setup method. It is responsible for setting up and installing the package.
    """
    setup(name=NAME,
          url='http://eskapade.kave.io',
          license='',
          author='KPMG',
          author_email='eskapade@eskapade',
          description='Eskapade test package',
          python_requires='>=3.5',
          packages=['eskapade_python'],
          install_requires=[]
          )


if __name__ == '__main__':
    setup_package()
