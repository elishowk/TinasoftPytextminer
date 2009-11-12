__author__="elishowk, jbilcke"
__date__ ="$Nov 6, 2009 3:08:00 PM$"

from setuptools import setup,find_packages

setup (
  name = 'PyTextMiner',
  version = '0.1',
  packages = find_packages(),

  # Declare your packages' dependencies here, for eg:
  install_requires=['tina-storage>=0.1', 'nltk=>2.0b6'],

  # Fill in these to make your Egg ready for upload to
  # PyPI
  author = 'jbilcke',
  author_email = 'julian.bilcke@iscpif.fr',

  summary = 'Just another Python package for the cheese shop',
  url = '',
  license = 'GNU GPL v3',
  long_description= 'A Text Mining module',

  # could also include long_description, download_url, classifiers, etc.

  
)
