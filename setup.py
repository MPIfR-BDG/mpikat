from setuptools import setup

setup(name='mpikat',
      version='0.1',
      description='FBFUSE and APSUSE interfaces for the MeerKAT CAM system',
      url='https://github.com/ewanbarr/mpikat',
      author='Ewan Barr',
      author_email='ebarr@mpifr-bonn.mpg.de',
      license='MIT',
      packages=['mpikat'],
      install_requires=[
          'katpoint',
          'katcp',
          'katportalclient',
          'mosaic'
      ],
      dependency_links=[
          'git+https://gitlab.mpifr-bonn.mpg.de/wchen/Beamforming.git' #mosaic
      ],
      zip_safe=False)
