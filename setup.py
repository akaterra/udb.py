import os
from setuptools import setup

README = open(os.path.join(os.path.dirname(__file__), 'readme.rst')).read()

setup(name='udbpy',
      version='0.1.0',
      description='In-memory database',
      long_description=README,
      long_description_content_type='text/x-rst',
      url='https://github.com/akaterra/udb.py',
      license='MIT',
      install_requires=[],
      zip_safe=False)
