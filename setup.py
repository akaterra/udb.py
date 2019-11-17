import os
from setuptools import setup

README = open(os.path.join(os.path.dirname(__file__), 'readme.rst')).read()

setup(name='udb_py',
      version='0.0.1',
      description='In-memory database',
      long_description=README,
      long_description_content_type='text/x-rst',
      url='https://github.com/akaterra/udb.py',
      license='MIT',
      install_requires=[],
      packages=['udb_py', 'udb_py.index', 'udb_py.storage'],
      zip_safe=False)
