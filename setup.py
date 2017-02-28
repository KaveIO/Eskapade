from distutils.core import setup

setup(
      name='Eskapade',
      version='0',
      description='Eskapade modular analytics',
      author='KPMG',
      url='http://eskapade.kave.io',
      packages=['eskapade',
                'eskapade.analysis',
                'eskapade.analysis.links',
                'eskapade.core',
                'eskapade.core_ops', 
                'eskapade.core_ops.links',
                'eskapade.visualization',
                'eskapade.visualization.links',
               ],
      package_dir={'': 'python', 
                   'eskapade': 'python/eskapade', 
                   'analysis': 'python/eskapade/analysis',
                   'core': 'python/eskapade/core',
                   'core_ops': 'python/eskapade/core_ops',
                   'visualization': 'python/eskapade/visualization',
                   }
)
