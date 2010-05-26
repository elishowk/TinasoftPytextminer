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

#from setuptools import setup,find_packages
from distutils.core import setup
from distutils import py2exe
import os
import glob

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEPS = glob.glob(os.path.dirname(os.path.abspath(__file__)) + "/deps/*")
data_files = [("Microsoft.VC90.CRT", glob(r'c:\ms-vc-runtime\*.*'))]

setup (
    name = 'TinasoftPytextminer',
    data_files=data_files,
    #packages = find_packages(),
    #include_package_data=True,
    # Declare your packages' dependencies here, for eg:
    #install_requires=['numpy','bsddb3','nltk','jsonpickle','tenjin'],
    dependency_links=DEPS,
    #package_data = {'tinasoft': ['shared', 'config.yaml', 'README', 'LICENSE']},
)
