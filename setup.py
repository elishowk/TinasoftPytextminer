#!/usr/env/python
# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

__version__="1.0alpha"

__url__="http://tinasoft.eu"
__longdescr__="A text-mining python module producing thematic networks"
__license__="GNU General Public License v3"
__keywords__="semantic network"
__author__="elias showk"
__author_email__="elishowk@nonutc.fr"

__classifiers__= [
"Topic :: Text Processing :: Indexing",
"Topic :: Text Processing :: Linguistic",
"Topic :: Internet :: WWW/HTTP :: HTTP Servers",
"Programming Language :: Python :: 2.6",
"Operating System :: Microsoft :: Windows",
"Operating System :: MacOS :: MacOS X",
"Operating System :: Unix",
"Framework :: Twisted"
]

from setuptools import find_packages
from distutils.core import setup
from glob import glob
import os
from os import path
#import unittest

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
#DEPS = glob(os.path.dirname(os.path.abspath(__file__)) + "/deps/*")

data_files = [
    ('tinasoft/shared', glob(r'shared/*.*')),
    ('tinasoft', glob(r'config_unix.yaml')),
    ('tinasoft', glob(r'config_win.yaml')),
    ('tinasoft',glob(r'README')),
    ('tinasoft',glob(r'LICENSE'))
]

setup (
    name = 'TinasoftPytextminer',
    packages = find_packages(),
    data_files = data_files,
    include_package_data = True,
    # Declare your packages' dependencies here, for eg:
    install_requires = ['simplejson','pyyaml','numpy','nltk','jsonpickle','tenjin','zope.interface','twisted'],
    scripts = ['httpserver.py'],
    version = __version__,
    url = __url__,
    long_description = __longdescr__,
    license = __license__,
    keywords = __keywords__,
    author = __author__,
    author_email = __author_email__,
    classifiers = __classifiers__,
    #test_suite = "unittest.TinasoftUnitTests",
    #dependency_links = DEPS,
)
