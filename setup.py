from setuptools import setup, find_packages

setup(name='mpikat',
      version='0.1',
      description='FBFUSE and APSUSE interfaces for the MeerKAT CAM system',
      url='https://github.com/ewanbarr/mpikat',
      author='Ewan Barr',
      author_email='ebarr@mpifr-bonn.mpg.de',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'katpoint',
          'katcp',
          'ipaddress',
          'katportalclient',
          'mosaic',
          'posix_ipc',
          'jinja2',
          'coloredlogs',
          'path.py'
      ],
      dependency_links=[
          'git+https://github.com/ska-sa/katportalclient.git',
          'git+https://gitlab.mpifr-bonn.mpg.de/wchen/Beamforming.git' #mosaic
      ],
      zip_safe=False)
