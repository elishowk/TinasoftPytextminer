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

__version__="1.0alpha6"
__url__="http://tinasoft.eu"
__longdescr__="A text-mining python module producing bottom-up thematic field recontruction"
__license__="GNU General Public License"
__keywords__="nlp,textmining,graph"
__maintainer__="elishowk@nonutc.fr"
__maintainer_email__="elishowk@nonutc.fr"
__author__="elias showk"
__author_email__="elishowk@nonutc.fr"
__classifiers__="nlp textmining http"

from setuptools import find_packages
from distutils.core import setup
try:
    import py2exe
except: pass
import os
from glob import glob

import unittest

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEPS = glob(os.path.dirname(os.path.abspath(__file__)) + "/deps/*")

#data_files = [("Microsoft.VC90.CRT", glob(r'e:\Microsoft.VC90.CRT\*.*')),
#('',glob(r'e:\TinasoftPytextminer\README')),
#('',glob(r'e:\TinasoftPytextminer\LICENSE')),
#('',glob(r'e:\TinasoftPytextminer\tinasoft\config.yaml'))
#]

data_files = [
    ('shared', glob(r'shared/*.*')),
    ('', glob(r'tinasoft/config.yaml')),
    ('',glob(r'README')),
    ('',glob(r'LICENSE')),
]

setup (
    name = 'TinasoftPytextminer',
    packages = find_packages(),
    data_files = data_files,
    include_package_data = True,
    # Declare your packages' dependencies here, for eg:
    install_requires = ['numpy','pyyaml','bsddb3','nltk','jsonpickle','tenjin','twisted','simplejson'],
    #dependency_links = DEPS,
    #console = ["httpserver.py"],
    #package_data = {'tinasoft': ['shared', 'config.yaml', 'README', 'LICENSE']},
    options = {"py2exe":
        {
        "bundle_files": 1,
        "includes": ['numpy','bsddb3','nltk','jsonpickle','yaml','tenjin','twisted','twisted.web','twisted.internet','twisted.web.resource','simplejson'],
        }
    },
    version = __version__,
    url = __url__,
    long_description = __longdescr__,
    license = __license__,
    keywords = __keywords__,
    maintainer = __maintainer__,
    maintainer_email = __maintainer_email__,
    author = __author__,
    author_email = __author_email__,
    classifiers = __classifiers__,
    #test_suite = "unittest.TinasoftUnitTests",
)
