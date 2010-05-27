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
__author__="elishowk"

from setuptools import find_packages
from distutils.core import setup
import py2exe
import os
from glob import glob

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEPS = glob(os.path.dirname(os.path.abspath(__file__)) + "/deps/*")
data_files = [("Microsoft.VC90.CRT", glob(r'e:\Microsoft.VC90.CRT\*.*')),
('',glob(r'e:\TinasoftPytextminer\README')),
('',glob(r'e:\TinasoftPytextminer\LICENSE')),
('',glob(r'e:\TinasoftPytextminer\config.yaml'))
]

#('shared', glob(r'e:\TinasoftPytextminer\shared\*.*')),

setup (
    name = 'TinasoftPytextminer',
    packages = find_packages(),
	data_files=data_files,
    include_package_data=True,
    # Declare your packages' dependencies here, for eg:
    install_requires=['numpy','yaml','bsddb3','nltk','jsonpickle','tenjin','twisted','simplejson'],
    #dependency_links=DEPS,
	console = ["tinaserver.py"],
    #package_data = {'tinasoft': ['shared', 'config.yaml', 'README', 'LICENSE']},
	options = {"py2exe": 
		{
		"bundle_files": 1,
		"includes":['numpy','bsddb3','nltk','jsonpickle','yaml','tenjin','twisted','twisted.web','twisted.internet','twisted.web.resource','simplejson'],
		}
	},
)
